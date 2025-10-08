//! Mini-Redis Server
//!
//! A minimal Redis server implementation demonstrating the use of zio's
//! Reader/Writer interfaces for protocol parsing and response building.
//!
//! Implements RESP2 (REdis Serialization Protocol) with the following commands:
//! - PING: Returns PONG
//! - ECHO <message>: Returns the message
//! - SET <key> <value> [EX seconds|PX milliseconds]: Stores a key-value pair with optional expiration
//! - GET <key>: Retrieves a value by key
//! - DEL <key>: Deletes a key
//! - EXISTS <key>: Checks if a key exists
//! - TTL <key>: Get remaining time to live in seconds
//! - PTTL <key>: Get remaining time to live in milliseconds
//! - EXPIRE <key> <seconds>: Set expiration on an existing key
//! - PEXPIRE <key> <milliseconds>: Set expiration on an existing key in milliseconds
//! - PERSIST <key>: Remove expiration from a key
//! - SETEX <key> <seconds> <value>: Set key with expiration in seconds
//! - PSETEX <key> <milliseconds> <value>: Set key with expiration in milliseconds
//!
//! Usage:
//!   zig build
//!   ./zig-out/bin/mini-redis
//!
//! Test with redis-cli:
//!   redis-cli -p 6379
//!   > PING
//!   PONG
//!   > SET greeting "Hello, World!"
//!   OK
//!   > GET greeting
//!   "Hello, World!"

const std = @import("std");
const zio = @import("zio");

// Protocol limits
const MAX_BULK_LEN = 64 * 1024; // 64KB max bulk string
const MAX_ARRAY_LEN = 128; // max command args

// Buffer sizes
const READ_BUFFER_SIZE = 64 * 1024; // 64KB for reading commands
const WRITE_BUFFER_SIZE = 16 * 1024; // 16KB for writing responses

// Connection limits
const MAX_CONNECTIONS: usize = 100; // Maximum concurrent connections

// =============================================================================
// StringRef - Reference-counted string with single allocation
// =============================================================================

/// A reference-counted string with single allocation for struct + data
const StringRef = struct {
    refcount: usize,
    len: usize,
    data: [0]u8,

    /// Create a new StringRef with refcount=1
    /// Allocates StringRef struct and string data in a single memory block
    fn create(allocator: std.mem.Allocator, str: []const u8) !*StringRef {
        // Calculate total size: StringRef struct + string data
        const total_size = @sizeOf(StringRef) + str.len;

        // Single allocation for both struct and data
        const bytes = try allocator.alignedAlloc(u8, std.mem.Alignment.fromByteUnits(@alignOf(StringRef)), total_size);
        errdefer allocator.free(bytes);

        // First part is the StringRef struct
        const ref: *StringRef = @ptrCast(bytes.ptr);

        // Initialize the struct
        ref.* = .{
            .refcount = 1,
            .len = str.len,
            .data = undefined,
        };

        // Second part is the string data (right after the struct)
        const data_ptr: [*]u8 = @ptrCast(&ref.data);
        const data_slice = data_ptr[0..str.len];
        @memcpy(data_slice, str);

        return ref;
    }

    /// Get data slice from StringRef
    fn getData(self: *const StringRef) []const u8 {
        const data_ptr: [*]const u8 = @ptrCast(&self.data);
        return data_ptr[0..self.len];
    }

    /// Get StringRef pointer from data slice
    fn fromData(data: []const u8) *StringRef {
        const field_ptr: *const [0]u8 = @ptrCast(data.ptr);
        const parent: *align(1) const StringRef = @fieldParentPtr("data", field_ptr);
        return @alignCast(@constCast(parent));
    }

    /// Increment the reference count
    fn borrow(self: *StringRef) void {
        self.refcount += 1;
    }

    /// Decrement the reference count and free if it reaches zero
    fn release(self: *StringRef, allocator: std.mem.Allocator) void {
        self.refcount -= 1;
        if (self.refcount == 0) {
            self.destroy(allocator);
        }
    }

    /// Destroy the StringRef (called when refcount reaches 0)
    fn destroy(self: *StringRef, allocator: std.mem.Allocator) void {
        // Reconstruct the original allocation from self pointer
        const bytes: [*]align(@alignOf(StringRef)) u8 = @ptrCast(self);
        const total_size = @sizeOf(StringRef) + self.len;
        allocator.free(bytes[0..total_size]);
    }
};

// =============================================================================
// Store - Key-Value Storage
// =============================================================================

/// Entry in the key-value store with optional expiration
const Entry = struct {
    value_ref: *StringRef,
    expires_at_ns: ?i64, // null = no expiration
};

/// Expiration tracking for a key
const Expiration = struct {
    when_ns: i64,
    key: []const u8, // Reference to key in Store.map (not owned)
};

fn compareExpirations(context: void, a: Expiration, b: Expiration) std.math.Order {
    _ = context;
    // First compare by time (primary sorting key for heap)
    const time_order = std.math.order(a.when_ns, b.when_ns);
    if (time_order != .eq) return time_order;

    // If times equal, compare by key pointer for uniqueness
    // This allows us to uniquely identify and remove specific expiration entries
    return std.math.order(@intFromPtr(a.key.ptr), @intFromPtr(b.key.ptr));
}

const Store = struct {
    map: std.StringHashMapUnmanaged(Entry),
    // NOTE: Using PriorityQueue requires O(n) search to remove specific entries.
    // Ideally we'd use a balanced tree for O(log n) removal, but there isn't one
    // in Zig's standard library and we want to keep this simple.
    expirations: std.PriorityQueue(Expiration, void, compareExpirations),

    // Synchronization for expiration worker
    mutex: zio.Mutex = zio.Mutex.init,
    expiration_cond: zio.Condition = zio.Condition.init,
    shutdown: bool = false,

    allocator: std.mem.Allocator,

    fn init(allocator: std.mem.Allocator) Store {
        return .{
            .map = .{},
            .expirations = std.PriorityQueue(Expiration, void, compareExpirations).init(allocator, {}),
            .allocator = allocator,
        };
    }

    fn deinit(self: *Store) void {
        var it = self.map.iterator();
        while (it.next()) |entry| {
            entry.value_ptr.*.value_ref.release(self.allocator);
            self.allocator.free(entry.key_ptr.*);
        }
        self.map.deinit(self.allocator);

        // Clean up expiration queue (keys are references, not owned)
        self.expirations.deinit();
    }

    /// Remove an expiration entry from the queue (O(n) search)
    fn removeExpiration(self: *Store, target: Expiration) void {
        var it = self.expirations.iterator();
        var idx: usize = 0;
        while (it.next()) |exp| : (idx += 1) {
            if (compareExpirations({}, exp, target) == .eq) {
                _ = self.expirations.removeIndex(idx);
                return;
            }
        }
    }

    /// Get an entry from the map, handling lazy expiration automatically
    fn getEntry(self: *Store, key: []const u8) ?std.StringHashMapUnmanaged(Entry).Entry {
        const entry = self.map.getEntry(key) orelse return null;

        // Check if expired (lazy expiration)
        if (entry.value_ptr.*.expires_at_ns) |expires_at| {
            if (std.time.nanoTimestamp() >= expires_at) {
                self.removeEntry(entry);
                return null;
            }
        }

        return entry;
    }

    fn set(self: *Store, rt: *zio.Runtime, key: []const u8, value: []const u8, expire_ms: ?u32) !void {
        self.mutex.lock(rt);
        defer self.mutex.unlock(rt);

        // Create new StringRef
        const value_ref = try StringRef.create(self.allocator, value);
        errdefer value_ref.release(self.allocator);

        // Calculate expiration time
        const expires_at_ns: ?i64 = if (expire_ms) |ms|
            @intCast(std.time.nanoTimestamp() + @as(i64, ms) * 1_000_000)
        else
            null;

        // Get or put the key
        const gop = try self.map.getOrPut(self.allocator, key);
        errdefer if (!gop.found_existing) self.map.removeByPtr(gop.key_ptr);

        var allocated_key: ?[]const u8 = null;
        errdefer if (allocated_key) |k| self.allocator.free(k);

        if (!gop.found_existing) {
            allocated_key = try self.allocator.dupe(u8, key);
            gop.key_ptr.* = allocated_key.?; 
        }

        var should_remove_expiration = true;
        if (expires_at_ns) |when_ns| {
            var should_add = true;
            if (gop.found_existing) {
                if (gop.value_ptr.*.expires_at_ns) |old_when_ns| {
                    if (when_ns == old_when_ns) {
                        // If new expiration is same, we don't need to add
                        should_add = false;
                        should_remove_expiration = false;
                    }
                }
            }
            if (should_add) {
                // Add to expiration queue
                try self.expirations.add(.{
                    .when_ns = when_ns,
                    .key = gop.key_ptr.*,
                });
                // Wake up expiration worker, it needs to update its sleep time
                self.expiration_cond.signal(rt);
            }
        }

        if (gop.found_existing) {
            // Remove old expiration entry if it exists
            if (gop.value_ptr.*.expires_at_ns) |old_when_ns| {
                if (should_remove_expiration) {
                    self.removeExpiration(.{ .when_ns = old_when_ns, .key = gop.key_ptr.*, });
                }
            }
            // Release old value
            gop.value_ptr.*.value_ref.release(self.allocator);
        }

        // Set new value
        gop.value_ptr.* = .{
            .value_ref = value_ref,
            .expires_at_ns = expires_at_ns,
        };
    }

    fn get(self: *Store, rt: *zio.Runtime, key: []const u8) ?[]const u8 {
        self.mutex.lock(rt);
        defer self.mutex.unlock(rt);

        const entry = self.getEntry(key) orelse return null;

        entry.value_ptr.*.value_ref.borrow();
        return entry.value_ptr.*.value_ref.getData();
    }

    fn releaseValue(self: *Store, rt: *zio.Runtime, value_data: []const u8) void {
        self.mutex.lock(rt);
        defer self.mutex.unlock(rt);

        const ref = StringRef.fromData(value_data);
        ref.release(self.allocator);
    }

    fn del(self: *Store, rt: *zio.Runtime, key: []const u8) bool {
        self.mutex.lock(rt);
        defer self.mutex.unlock(rt);

        const entry = self.getEntry(key) orelse return false;

        self.removeEntry(entry);
        return true;
    }

    fn exists(self: *Store, rt: *zio.Runtime, key: []const u8) bool {
        self.mutex.lock(rt);
        defer self.mutex.unlock(rt);

        return self.getEntry(key) != null;
    }

    /// Get TTL for a key in nanoseconds
    /// Returns: nanoseconds remaining, -1 if no expiration, -2 if key doesn't exist
    fn getTtl(self: *Store, rt: *zio.Runtime, key: []const u8) i64 {
        self.mutex.lock(rt);
        defer self.mutex.unlock(rt);

        const entry = self.getEntry(key) orelse return -2;

        if (entry.value_ptr.expires_at_ns) |expires_at| {
            const now = std.time.nanoTimestamp();
            const remaining: i64 = @intCast(expires_at - now);
            return if (remaining > 0) remaining else -2; // Already expired
        }

        return -1; // No expiration
    }

    /// Set expiration on an existing key
    /// Returns true if expiration was set, false if key doesn't exist
    fn expire(self: *Store, rt: *zio.Runtime, key: []const u8, expire_ms: u32) !bool {
        self.mutex.lock(rt);
        defer self.mutex.unlock(rt);

        const entry = self.getEntry(key) orelse return false;

        const expires_at_ns: i64 = @intCast(std.time.nanoTimestamp() + @as(i64, expire_ms) * 1_000_000);

        // Remove old expiration if it exists
        if (entry.value_ptr.expires_at_ns) |old_when_ns| {
            // Only remove if different
            if (old_when_ns != expires_at_ns) {
                self.removeExpiration(.{ .when_ns = old_when_ns, .key = entry.key_ptr.* });
            } else {
                // Same expiration time, no need to update
                return true;
            }
        }

        // Add new expiration
        try self.expirations.add(.{
            .when_ns = expires_at_ns,
            .key = entry.key_ptr.*,
        });

        // Update entry
        entry.value_ptr.expires_at_ns = expires_at_ns;

        // Wake up expiration worker
        self.expiration_cond.signal(rt);

        return true;
    }

    /// Remove expiration from a key
    /// Returns true if expiration was removed, false if key doesn't exist or had no expiration
    fn persist(self: *Store, rt: *zio.Runtime, key: []const u8) bool {
        self.mutex.lock(rt);
        defer self.mutex.unlock(rt);

        const entry = self.getEntry(key) orelse return false;

        if (entry.value_ptr.expires_at_ns) |expires_at| {
            self.removeExpiration(.{ .when_ns = expires_at, .key = entry.key_ptr.* });
            entry.value_ptr.expires_at_ns = null;
            return true;
        }

        return false; // No expiration to remove
    }

    fn removeEntry(self: *Store, entry: std.StringHashMapUnmanaged(Entry).Entry) void {
        if (entry.value_ptr.expires_at_ns) |expires_at| {
            self.removeExpiration(.{ .when_ns = expires_at, .key = entry.key_ptr.* });
        }
        entry.value_ptr.value_ref.release(self.allocator);
        self.allocator.free(entry.key_ptr.*);
        self.map.removeByPtr(entry.key_ptr);
    }
};

// =============================================================================
// Expiration Worker
// =============================================================================

/// Background task that purges expired keys
fn expirationWorker(rt: *zio.Runtime, store: *Store) void {
    while (true) {
        store.mutex.lock(rt);
        defer store.mutex.unlock(rt);

        if (store.shutdown) {
            break;
        }

        // Purge all expired keys
        const now = std.time.nanoTimestamp();
        while (store.expirations.peek()) |exp| {
            if (exp.when_ns > now) break; // Not expired yet

            const expired = store.expirations.remove();
            if (store.map.fetchRemove(expired.key)) |kv| {
                kv.value.value_ref.release(store.allocator);
                store.allocator.free(kv.key);
            }
            // Note: expired.key is a reference, not owned, so don't free it
        }

        // Wait for next expiration OR notification
        if (store.expirations.peek()) |next| {
            const timeout_ns = next.when_ns - std.time.nanoTimestamp();
            if (timeout_ns > 0) {
                // timedWait = tokio::select! equivalent
                // Returns error.Timeout OR wakes on signal
                // NOTE: timedWait releases and reacquires mutex internally
                store.expiration_cond.timedWait(rt, &store.mutex, @intCast(timeout_ns)) catch {
                    // Timeout - loop back to purge
                };
                // If signaled early - loop back to check new expiration
            }
        } else {
            // No expirations - wait indefinitely for notification
            // NOTE: wait releases and reacquires mutex internally
            store.expiration_cond.wait(rt, &store.mutex);
        }
    }

    std.log.debug("Expiration worker shut down", .{});
}

// =============================================================================
// RESP2 Protocol Parser
// =============================================================================

const Command = struct {
    args: [][]u8,
    arena: std.heap.ArenaAllocator,

    fn deinit(self: *Command) void {
        self.arena.deinit();
    }
};

const RespParser = struct {
    reader: *std.io.Reader,
    allocator: std.mem.Allocator,

    /// Parse one command (array of bulk strings)
    fn parseCommand(self: *RespParser) !Command {
        // Read '*' + count + \r\n
        const star = try self.reader.takeByte();
        if (star != '*') return error.ProtocolError;

        const count_line = try self.reader.takeDelimiterExclusive('\n');
        if (count_line.len == 0 or count_line[count_line.len - 1] != '\r') {
            return error.ProtocolError;
        }

        const count = try std.fmt.parseInt(usize, count_line[0 .. count_line.len - 1], 10);
        if (count == 0 or count > MAX_ARRAY_LEN) return error.ProtocolError;

        // Create arena for this command's allocations
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        errdefer arena.deinit();

        const arena_allocator = arena.allocator();

        // Parse each bulk string
        const args = try arena_allocator.alloc([]u8, count);
        var i: usize = 0;

        while (i < count) : (i += 1) {
            args[i] = try self.parseBulkString(arena_allocator);
        }

        return Command{ .args = args, .arena = arena };
    }

    /// Parse $<len>\r\n<data>\r\n
    fn parseBulkString(self: *RespParser, allocator: std.mem.Allocator) ![]u8 {
        const dollar = try self.reader.takeByte();
        if (dollar != '$') return error.ProtocolError;

        const len_line = try self.reader.takeDelimiterExclusive('\n');
        if (len_line.len == 0 or len_line[len_line.len - 1] != '\r') {
            return error.ProtocolError;
        }

        const len = try std.fmt.parseInt(usize, len_line[0 .. len_line.len - 1], 10);
        if (len > MAX_BULK_LEN) return error.BulkStringTooLarge;

        // Read data + \r\n
        const data = try self.reader.take(len);
        const owned = try allocator.dupe(u8, data);

        const cr = try self.reader.takeByte();
        const lf = try self.reader.takeByte();
        if (cr != '\r' or lf != '\n') return error.ProtocolError;

        return owned;
    }
};

// =============================================================================
// RESP2 Response Writer
// =============================================================================

const RespWriter = struct {
    writer: *std.io.Writer,

    /// Write +OK\r\n style response
    fn writeSimpleString(self: *RespWriter, str: []const u8) !void {
        try self.writer.writeByte('+');
        try self.writer.writeAll(str);
        try self.writer.writeAll("\r\n");
    }

    /// Write -ERR message\r\n style response
    fn writeError(self: *RespWriter, msg: []const u8) !void {
        try self.writer.writeByte('-');
        try self.writer.writeAll(msg);
        try self.writer.writeAll("\r\n");
    }

    /// Write :123\r\n style response
    fn writeInteger(self: *RespWriter, n: i64) !void {
        try self.writer.writeByte(':');
        try self.writer.print("{d}\r\n", .{n});
    }

    /// Write $6\r\nfoobar\r\n style response
    fn writeBulkString(self: *RespWriter, str: []const u8) !void {
        try self.writer.print("${d}\r\n", .{str.len});
        try self.writer.writeAll(str);
        try self.writer.writeAll("\r\n");
    }

    /// Write $-1\r\n (null bulk string)
    fn writeNull(self: *RespWriter) !void {
        try self.writer.writeAll("$-1\r\n");
    }
};

// =============================================================================
// Command Handlers
// =============================================================================

const CommandHandler = struct {
    store: *Store,
    runtime: *zio.Runtime,

    fn execute(self: *CommandHandler, cmd: *Command, resp: *RespWriter) !void {
        if (cmd.args.len == 0) return error.EmptyCommand;

        const command_name = cmd.args[0];

        // Convert command to uppercase for case-insensitive matching
        var upper_buf: [32]u8 = undefined;
        if (command_name.len > upper_buf.len) {
            return resp.writeError("ERR command name too long");
        }
        const upper_cmd = std.ascii.upperString(&upper_buf, command_name);

        if (std.mem.eql(u8, upper_cmd, "PING")) {
            return self.handlePing(cmd, resp);
        } else if (std.mem.eql(u8, upper_cmd, "ECHO")) {
            return self.handleEcho(cmd, resp);
        } else if (std.mem.eql(u8, upper_cmd, "SET")) {
            return self.handleSet(cmd, resp);
        } else if (std.mem.eql(u8, upper_cmd, "GET")) {
            return self.handleGet(cmd, resp);
        } else if (std.mem.eql(u8, upper_cmd, "DEL")) {
            return self.handleDel(cmd, resp);
        } else if (std.mem.eql(u8, upper_cmd, "EXISTS")) {
            return self.handleExists(cmd, resp);
        } else if (std.mem.eql(u8, upper_cmd, "TTL")) {
            return self.handleTtl(cmd, resp);
        } else if (std.mem.eql(u8, upper_cmd, "PTTL")) {
            return self.handlePttl(cmd, resp);
        } else if (std.mem.eql(u8, upper_cmd, "EXPIRE")) {
            return self.handleExpire(cmd, resp);
        } else if (std.mem.eql(u8, upper_cmd, "PEXPIRE")) {
            return self.handlePexpire(cmd, resp);
        } else if (std.mem.eql(u8, upper_cmd, "PERSIST")) {
            return self.handlePersist(cmd, resp);
        } else if (std.mem.eql(u8, upper_cmd, "SETEX")) {
            return self.handleSetex(cmd, resp);
        } else if (std.mem.eql(u8, upper_cmd, "PSETEX")) {
            return self.handlePsetex(cmd, resp);
        } else {
            try resp.writeError("ERR unknown command");
        }
    }

    fn handlePing(self: *CommandHandler, cmd: *Command, resp: *RespWriter) !void {
        _ = self;
        if (cmd.args.len != 1) {
            return resp.writeError("ERR wrong number of arguments for 'ping' command");
        }
        try resp.writeSimpleString("PONG");
    }

    fn handleEcho(self: *CommandHandler, cmd: *Command, resp: *RespWriter) !void {
        _ = self;
        if (cmd.args.len != 2) {
            return resp.writeError("ERR wrong number of arguments for 'echo' command");
        }
        try resp.writeBulkString(cmd.args[1]);
    }

    fn handleSet(self: *CommandHandler, cmd: *Command, resp: *RespWriter) !void {
        if (cmd.args.len < 3) {
            return resp.writeError("ERR wrong number of arguments for 'set' command");
        }

        var expire_ms: ?u32 = null;

        // Parse optional expiration: SET key value [EX seconds|PX milliseconds]
        if (cmd.args.len >= 5) {
            const option = cmd.args[3];
            if (std.ascii.eqlIgnoreCase(option, "EX")) {
                const seconds = std.fmt.parseInt(u32, cmd.args[4], 10) catch {
                    return resp.writeError("ERR invalid expire time in 'set' command");
                };
                expire_ms = seconds * 1000;
            } else if (std.ascii.eqlIgnoreCase(option, "PX")) {
                expire_ms = std.fmt.parseInt(u32, cmd.args[4], 10) catch {
                    return resp.writeError("ERR invalid expire time in 'set' command");
                };
            } else {
                return resp.writeError("ERR syntax error");
            }
        } else if (cmd.args.len == 4) {
            return resp.writeError("ERR syntax error");
        }

        try self.store.set(self.runtime, cmd.args[1], cmd.args[2], expire_ms);
        try resp.writeSimpleString("OK");
    }

    fn handleGet(self: *CommandHandler, cmd: *Command, resp: *RespWriter) !void {
        if (cmd.args.len != 2) {
            return resp.writeError("ERR wrong number of arguments for 'get' command");
        }
        if (self.store.get(self.runtime, cmd.args[1])) |value_data| {
            defer self.store.releaseValue(self.runtime, value_data);
            try resp.writeBulkString(value_data);
        } else {
            try resp.writeNull();
        }
    }

    fn handleDel(self: *CommandHandler, cmd: *Command, resp: *RespWriter) !void {
        if (cmd.args.len != 2) {
            return resp.writeError("ERR wrong number of arguments for 'del' command");
        }
        const deleted = self.store.del(self.runtime, cmd.args[1]);
        try resp.writeInteger(if (deleted) 1 else 0);
    }

    fn handleExists(self: *CommandHandler, cmd: *Command, resp: *RespWriter) !void {
        if (cmd.args.len != 2) {
            return resp.writeError("ERR wrong number of arguments for 'exists' command");
        }
        const exists_result = self.store.exists(self.runtime, cmd.args[1]);
        try resp.writeInteger(if (exists_result) 1 else 0);
    }

    fn handleTtl(self: *CommandHandler, cmd: *Command, resp: *RespWriter) !void {
        if (cmd.args.len != 2) {
            return resp.writeError("ERR wrong number of arguments for 'ttl' command");
        }
        const ttl_ns = self.store.getTtl(self.runtime, cmd.args[1]);
        if (ttl_ns == -2 or ttl_ns == -1) {
            try resp.writeInteger(ttl_ns);
        } else {
            // Convert nanoseconds to seconds
            const ttl_s: i64 = @intCast(@divFloor(ttl_ns, 1_000_000_000));
            try resp.writeInteger(ttl_s);
        }
    }

    fn handlePttl(self: *CommandHandler, cmd: *Command, resp: *RespWriter) !void {
        if (cmd.args.len != 2) {
            return resp.writeError("ERR wrong number of arguments for 'pttl' command");
        }
        const ttl_ns = self.store.getTtl(self.runtime, cmd.args[1]);
        if (ttl_ns == -2 or ttl_ns == -1) {
            try resp.writeInteger(ttl_ns);
        } else {
            // Convert nanoseconds to milliseconds
            const ttl_ms: i64 = @intCast(@divFloor(ttl_ns, 1_000_000));
            try resp.writeInteger(ttl_ms);
        }
    }

    fn handleExpire(self: *CommandHandler, cmd: *Command, resp: *RespWriter) !void {
        if (cmd.args.len != 3) {
            return resp.writeError("ERR wrong number of arguments for 'expire' command");
        }
        const seconds = std.fmt.parseInt(u32, cmd.args[2], 10) catch {
            return resp.writeError("ERR value is not an integer or out of range");
        };
        const expire_ms = seconds * 1000;
        const success = try self.store.expire(self.runtime, cmd.args[1], expire_ms);
        try resp.writeInteger(if (success) 1 else 0);
    }

    fn handlePexpire(self: *CommandHandler, cmd: *Command, resp: *RespWriter) !void {
        if (cmd.args.len != 3) {
            return resp.writeError("ERR wrong number of arguments for 'pexpire' command");
        }
        const milliseconds = std.fmt.parseInt(u32, cmd.args[2], 10) catch {
            return resp.writeError("ERR value is not an integer or out of range");
        };
        const success = try self.store.expire(self.runtime, cmd.args[1], milliseconds);
        try resp.writeInteger(if (success) 1 else 0);
    }

    fn handlePersist(self: *CommandHandler, cmd: *Command, resp: *RespWriter) !void {
        if (cmd.args.len != 2) {
            return resp.writeError("ERR wrong number of arguments for 'persist' command");
        }
        const success = self.store.persist(self.runtime, cmd.args[1]);
        try resp.writeInteger(if (success) 1 else 0);
    }

    fn handleSetex(self: *CommandHandler, cmd: *Command, resp: *RespWriter) !void {
        if (cmd.args.len != 4) {
            return resp.writeError("ERR wrong number of arguments for 'setex' command");
        }
        const seconds = std.fmt.parseInt(u32, cmd.args[2], 10) catch {
            return resp.writeError("ERR value is not an integer or out of range");
        };
        const expire_ms = seconds * 1000;
        try self.store.set(self.runtime, cmd.args[1], cmd.args[3], expire_ms);
        try resp.writeSimpleString("OK");
    }

    fn handlePsetex(self: *CommandHandler, cmd: *Command, resp: *RespWriter) !void {
        if (cmd.args.len != 4) {
            return resp.writeError("ERR wrong number of arguments for 'psetex' command");
        }
        const milliseconds = std.fmt.parseInt(u32, cmd.args[2], 10) catch {
            return resp.writeError("ERR value is not an integer or out of range");
        };
        try self.store.set(self.runtime, cmd.args[1], cmd.args[3], milliseconds);
        try resp.writeSimpleString("OK");
    }
};

// =============================================================================
// Connection Handler
// =============================================================================

const ConnectionHandler = struct {
    stream: zio.TcpStream,
    store: *Store,
    runtime: *zio.Runtime,
    allocator: std.mem.Allocator,

    fn run(rt: *zio.Runtime, stream: zio.TcpStream, store_ptr: *Store, alloc: std.mem.Allocator, semaphore: *zio.Semaphore) !void {
        defer semaphore.post(rt);

        var self = ConnectionHandler{
            .stream = stream,
            .store = store_ptr,
            .runtime = rt,
            .allocator = alloc,
        };
        defer self.stream.close();

        self.handle() catch |err| {
            std.log.debug("Connection handler error: {}", .{err});
        };
    }

    fn handle(self: *ConnectionHandler) !void {
        const read_buffer = try self.allocator.alloc(u8, READ_BUFFER_SIZE);
        defer self.allocator.free(read_buffer);

        const write_buffer = try self.allocator.alloc(u8, WRITE_BUFFER_SIZE);
        defer self.allocator.free(write_buffer);

        var reader = self.stream.reader(read_buffer);
        var writer = self.stream.writer(write_buffer);

        var parser = RespParser{
            .reader = &reader.interface,
            .allocator = self.allocator,
        };

        var resp_writer = RespWriter{ .writer = &writer.interface };
        var handler = CommandHandler{ .store = self.store, .runtime = self.runtime };

        while (true) {
            var cmd = parser.parseCommand() catch |err| {
                switch (err) {
                    error.EndOfStream => break,
                    error.ProtocolError => {
                        try resp_writer.writeError("ERR Protocol error");
                        try writer.interface.flush();
                        continue;
                    },
                    error.BulkStringTooLarge => {
                        try resp_writer.writeError("ERR Bulk string too large");
                        try writer.interface.flush();
                        continue;
                    },
                    else => {
                        try resp_writer.writeError("ERR Internal error");
                        try writer.interface.flush();
                        return err;
                    },
                }
            };
            defer cmd.deinit();

            handler.execute(&cmd, &resp_writer) catch |err| {
                std.log.err("Command execution error: {}", .{err});
                try resp_writer.writeError("ERR Command failed");
            };

            try writer.interface.flush();
        }
    }
};

// =============================================================================
// Main Server
// =============================================================================

fn runServer(rt: *zio.Runtime, store_ptr: *Store, alloc: std.mem.Allocator) !void {
    const addr = try zio.Address.parseIp4("127.0.0.1", 6379);
    var listener = try zio.TcpListener.init(rt, addr);
    defer listener.close();

    try listener.bind(addr);
    try listener.listen(128);

    // Spawn background expiration worker
    var expiration_task = try rt.spawn(expirationWorker, .{ rt, store_ptr }, .{});
    defer {
        // Signal shutdown to expiration worker
        store_ptr.mutex.lock(rt);
        store_ptr.shutdown = true;
        store_ptr.expiration_cond.signal(rt);
        store_ptr.mutex.unlock(rt);
        expiration_task.deinit();
    }

    // Initialize semaphore with MAX_CONNECTIONS permits
    var connection_limiter = zio.Semaphore{ .permits = MAX_CONNECTIONS };

    std.log.info("Mini-Redis server listening on 127.0.0.1:6379 (max {d} connections)", .{MAX_CONNECTIONS});
    std.log.info("Test with: redis-cli -p 6379", .{});

    while (true) {
        // Wait for a permit to become available
        //
        // If none are available, the listener waits for one.
        // When handlers complete processing a connection, the permit is returned
        // to the semaphore.
        connection_limiter.wait(rt);
        var permit_released = false;
        errdefer if (!permit_released) connection_limiter.post(rt);

        var stream = try listener.accept();
        errdefer stream.close();
        var handle = try rt.spawn(ConnectionHandler.run, .{ rt, stream, store_ptr, alloc, &connection_limiter }, .{});
        permit_released = true;
        handle.deinit();
    }
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var runtime = try zio.Runtime.init(allocator, .{});
    defer runtime.deinit();

    var store = Store.init(allocator);
    defer store.deinit();

    try runtime.runUntilComplete(runServer, .{ &runtime, &store, allocator }, .{});
}
