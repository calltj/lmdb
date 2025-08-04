const express = require("express");
const { open } = require("lmdb");
const { MongoClient } = require("mongodb");
const mysql = require("mysql2/promise");
const cors = require("cors");
const fs = require("fs");
const cron = require("node-cron");
const path = require("path");
const { DateTime } = require("luxon");
const { Client: PgClient } = require("pg");
// const Aerospike = require("aerospike");
const cassandra = require("cassandra-driver");
require("dotenv").config();

console.log("Nigeria time:", DateTime.now().setZone("Africa/Lagos").toISO());

const pool = require("./db");

const app = express();
app.use(express.json());
app.use(cors());

const LMDB_BASE = "E:/lmdb-cache";
let dbCurrent, dbPrev;
let mongoCollection, mysqlConn;

// Utility function: Formats date for folder naming
function folderName(dateTime) {
  return `lmdb_${dateTime.toISODate()}`;
}

// Open LMDB instance with compression
function openLMDB(dir) {
  return open({
    path: path.join(LMDB_BASE, dir),
    compression: true,
    useVersions: true,
    mapSize: 1024 * 1024 * 1024,
  });
}

// Rotate LMDBs: used daily to switch data files
function rotateLMDBs() {
  const todayNG = DateTime.now().setZone("Africa/Lagos").startOf("day");
  const tomorrowNG = todayNG.plus({ days: 1 });
  const yesterdayNG = todayNG.minus({ days: 1 });

  const oldDbPrev = dbPrev;

  const currentDir = folderName(tomorrowNG);
  const prevDir = folderName(todayNG);
  const toDeleteDir = path.join(LMDB_BASE, folderName(yesterdayNG));

  dbCurrent = openLMDB(currentDir);
  dbPrev = openLMDB(prevDir);

  if (oldDbPrev && typeof oldDbPrev.close === "function") {
    oldDbPrev.close();
  }

  try {
    fs.rmSync(toDeleteDir, { recursive: true, force: true });
    console.log(`[🧹] Deleted old LMDB folder: ${toDeleteDir}`);
  } catch (err) {
    console.warn(
      `[⚠️] Could not delete old LMDB: ${toDeleteDir}, ${err.message}`
    );
  }

  console.log(`[🔁] dbCurrent = ${currentDir}, dbPrev = ${prevDir}`);
}

// Get current active LMDB handle
const getActiveDB = () => dbCurrent;

// Query data from MongoDB or MySQL depending on app
async function query(appName, filter) {
  if (!filter) return null;

  if (appName === "rivas") {
    const mongoFilter = filter.userId
      ? { userId: filter.userId }
      : { email: filter.email };
    return await mongoCollection.findOne(mongoFilter);
  }

  if (appName === "ecommerce") {
    const sqlQuery = filter.userId
      ? ["SELECT * FROM users WHERE userId = ?", [filter.userId]]
      : ["SELECT * FROM users WHERE email = ?", [filter.email]];
    const [rows] = await mysqlConn.query(...sqlQuery);
    return rows[0] || null;
  }

  if (appName === "fast-store") {
    // YugabyteDB query
    const res = await yugaConn.query(
      "SELECT * FROM users WHERE userId = $1 OR email = $2",
      [filter.userId || "", filter.email || ""]
    );
    return res.rows[0] || null; // Return the first row or null if no results
  }

  // if (appName === "aerostore") {
  //   // Aerospike query
  //   const key = new Aerospike.Key(
  //     "test",
  //     "users",
  //     filter.userId || filter.email
  //   );
  //   try {
  //     const rec = await aeroClient.get(key); // Get the user record
  //     return rec.bins; // Return the record's bins (data)
  //   } catch (err) {
  //     return null; // Return null if record not found
  //   }
  // }

  if (appName === "scyllaapp") {
    // ScyllaDB query
    const q = "SELECT * FROM users WHERE userId = ?"; // Query template
    const res = await scyllaConn.execute(q, [filter.userId], { prepare: true });
    return res.rows[0] || null; // Return first result or null
  }

  return null;
}

// Route: Create or get user identity
app.post("/identity", async (req, res) => {
  const { user } = req.body;
  const appName = req.headers["x-app-name"];
  if (!user || !appName)
    return res.status(400).json({ error: "Missing user or app name" });

  const db = getActiveDB();
  const emailKey = `email:${user.email}`;
  const idKey = `user:${user.userId}`;

  try {
    const cached = db.get(idKey) || db.get(emailKey);
    if (cached) return res.json({ user: cached });

    const result = await query(appName, user);
    if (result) {
      db.put(idKey, result);
      db.put(emailKey, result);
      return res.json({ user: result });
    }

    const newUser = {
      ...user,
      balance: user.balance || 0,
      lastSyncedAt: null,
    };

    db.put(idKey, newUser);
    db.put(emailKey, newUser);
    return res.status(201).json({ user: newUser });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

// Route: Authenticate user by email
app.post("/auth", async (req, res) => {
  const { email } = req.body;
  const appName = req.headers["x-app-name"];
  if (!email || !appName)
    return res.status(400).json({ error: "Missing email or app name" });

  const db = getActiveDB();
  const emailKey = `email:${email}`;
  const cached = db.get(emailKey);
  if (cached) return res.json({ user: cached });

  try {
    const result = await query(appName, { email });
    if (result) {
      db.put(`email:${result.email}`, result);
      db.put(`user:${result.userId}`, result);
      return res.json({ user: result });
    }
    return res.status(404).json({ error: "User not found." });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

// Route: Check if email exists across DBs
app.get("/check", async (req, res) => {
  const { email } = req.query;
  if (!email) return res.status(400).json({ error: "Email required" });

  const db = getActiveDB();
  if (!db) {
    console.error("[❌] Active LMDB is not loaded!");
    return res.status(500).json({ error: "Internal DB error" });
  }

  const emailKey = `email:${email}`;
  if (db.get(emailKey)) return res.json({ exists: true, source: "lmdb" });

  try {
    const mongoUser = await mongoCollection.findOne({ email });
    if (mongoUser) return res.json({ exists: true, source: "mongodb" });
  } catch (err) {
    return res
      .status(500)
      .json({ error: "MongoDB error", details: err.message });
  }

  try {
    const [rows] = await pool.query("SELECT * FROM users WHERE email = ?", [
      email,
    ]);
    if (rows.length > 0) return res.json({ exists: true, source: "mysql" });
  } catch (err) {
    return res.status(500).json({ error: "MySQL error", details: err.message });
  }

  return res.json({ exists: false });
});

// Route: View all synced user records
app.get("/synced-records", async (_req, res) => {
  const records = [];
  for (const { key, value } of dbPrev.getRange({})) {
    if (key.startsWith("user:")) {
      records.push({
        userId: value.userId,
        lastSyncedAt: value.lastSyncedAt || null,
      });
    }
  }
  res.json(records);
});

// Route: Manual data sync
app.post("/sync", async (req, res) => {
  const batchSize = parseInt(req.query.batchSize || "100");
  try {
    const log = await fullSync(batchSize);
    res.json({ message: "Manual sync complete", entries: log.length });
  } catch (err) {
    res.status(500).json({ error: "Manual sync failed", details: err.message });
  }
});

// Function: Full sync from dbPrev to MongoDB/MySQL
async function fullSync(batchSize = 100) {
  const startTime = DateTime.now().setZone("Africa/Lagos");
  console.log(`[🚀] Sync started at ${startTime.toISOTime()} (Africa/Lagos)`);

  const entries = Array.from(dbPrev.getRange({})).filter((e) =>
    e.key.startsWith("user:")
  );
  const total = entries.length;
  const log = [];

  for (let i = 0; i < total; i += batchSize) {
    const batch = entries.slice(i, i + batchSize);
    for (let j = 0; j < batch.length; j++) {
      const { key, value } = batch[j];
      const app =
        value.app || (value.userId.startsWith("rivas") ? "rivas" : "ecommerce");
      value.lastSyncedAt = DateTime.now().toISO();

      if (app === "rivas") {
        // Clone and strip _id to avoid trying to overwrite it
        const { _id, ...safeValue } = value;

        await mongoCollection.updateOne(
          { userId: value.userId },
          { $set: safeValue },
          { upsert: true }
        );
        if (appName === "fast-store") {
          // Save to YugabyteDB
          await yugaConn.query(
            `INSERT INTO users (userId,name,email,age,balance,lastSyncedAt)
         VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT (userId) DO UPDATE
         SET name=$2,age=$4,balance=$5,lastSyncedAt=$6`,
            [
              value.userId, // userId
              value.name, // name
              value.email, // email
              value.age, // age
              value.balance, // balance
              value.lastSyncedAt, // last sync timestamp
            ]
          );
        }

        // if (appName === "aerostore") {
        //   // Save to Aerospike
        //   const key = new Aerospike.Key("test", "users", value.userId);
        //   await aeroClient.put(key, value); // Save the user record
        // }

        if (appName === "scyllaapp") {
          // Save to ScyllaDB
          await scyllaConn.execute(
            `INSERT INTO users (userId,name,email,age,balance,lastSyncedAt)
         VALUES(?,?,?,?,?,?)`,
            [
              value.userId, // userId
              value.name, // name
              value.email, // email
              value.age, // age
              value.balance, // balance
              value.lastSyncedAt, // last sync timestamp
            ],
            { prepare: true } // Prepared statement for performance
          );
        }
      } else {
        await mysqlConn.query(
          `INSERT INTO users (userId, name, email, age, balance)
           VALUES (?, ?, ?, ?, ?)
           ON DUPLICATE KEY UPDATE name = ?, age = ?, balance = ?`,
          [
            value.userId,
            value.name,
            value.email,
            value.age,
            value.balance || 0,
            value.name,
            value.age,
            value.balance || 0,
          ]
        );
      }

      dbPrev.remove(key);
      dbPrev.remove(`email:${value.email}`);
      log.push(`[SYNCED] ${value.userId}`);
    }
  }

  const endTime = DateTime.now().setZone("Africa/Lagos");
  const durationMs = endTime.diff(startTime).toObject().milliseconds;
  const summary = `[✅] Sync completed at ${endTime.toISOTime()} (Africa/Lagos) [${durationMs}ms elapsed]`;

  console.log(summary);
  fs.appendFileSync(
    "./sync_logs.txt",
    `${startTime.toISO()}\n${log.join("\n")}\n${summary}\n\n`
  );

  return log;
}

// 🔁 Inconsistency checker every 10 minutes
cron.schedule("*/10 * * * *", async () => {
  console.log("[🧠] Checking dbPrev for outdated data...");
  for (const { key, value } of dbPrev.getRange({})) {
    if (!key.startsWith("user:")) continue;
    const app =
      value.app || (value.userId.startsWith("rivas") ? "rivas" : "ecommerce");
    const liveRecord = await query(app, { userId: value.userId });
    if (!liveRecord || JSON.stringify(liveRecord) !== JSON.stringify(value)) {
      console.log(`[🔄 RESYNC REQUIRED] ${value.userId}`);
      await fullSync();
      break;
    }
  }
});

// 🌙 Nightly full sync job (11PM daily)
cron.schedule("35 12 * * *", async () => {
  console.log("[🕛] Nightly sync window (11PM–11:59PM) active...");
  await fullSync();
  console.log("[✅] Nightly sync done.");
});

// 🔁 Rotate LMDBs daily at 10:30PM
cron.schedule("51 11 * * *", () => {
  console.log("[🧭] Rotating LMDBs at 10:30 PM...");
  rotateLMDBs();
});

// Server startup block
(async () => {
  try {
    // Connect to MongoDB
    const mongoClient = new MongoClient("mongodb://localhost:27017");
    await mongoClient.connect();
    mongoCollection = mongoClient.db("rivas_db").collection("users");

    // Connect to MySQL
    mysqlConn = await mysql.createConnection({
      host: process.env.DB_HOST,
      user: process.env.DB_USER,
      password: process.env.DB_PASSWORD,
      database: process.env.DB_NAME,
      port: process.env.DB_PORT,
    });
    console.log("Connected to MySQL:", mysqlConn.config.database);

    yugaConn = new PgClient({
      host: process.env.YUGA_HOST,
      port: process.env.YUGA_PORT,
      user: process.env.YUGA_USER,
      password: process.env.YUGA_PASSWORD,
      database: process.env.YUGA_DB,
    });
    await yugaConn.connect(); // Make that Yugabyte connection
    console.log("✅ Connected to YugabyteDB");

    // aeroClient = await Aerospike.connect({
    //   hosts: "127.0.0.1:3000", // Aerospike host and port
    // });
    // console.log("✅ Connected to Aerospike");

    scyllaConn = new cassandra.Client({
      contactPoints: ["127.0.0.1"], // ScyllaDB contact point
      localDataCenter: "datacenter1", // ScyllaDB datacenter name
      keyspace: "scylla_keyspace", // Your ScyllaDB keyspace
    });
    console.log("✅ Connected to ScyllaDB");

    // Decide which LMDBs to load based on time
    const nowNG = DateTime.now().setZone("Africa/Lagos");
    const rotationHour = 11;
    const rotationMinute = 51;
    const todayNG = nowNG.startOf("day");

    let dbCurrentDate, dbPrevDate;

    if (
      nowNG.hour > rotationHour ||
      (nowNG.hour === rotationHour && nowNG.minute >= rotationMinute)
    ) {
      dbCurrentDate = todayNG.plus({ days: 1 });
      dbPrevDate = todayNG;
    } else {
      dbCurrentDate = todayNG;
      dbPrevDate = todayNG.minus({ days: 1 });
    }

    const currentDir = folderName(dbCurrentDate);
    const prevDir = folderName(dbPrevDate);
    dbCurrent = openLMDB(currentDir);
    dbPrev = openLMDB(prevDir);

    console.log(`[🔁 INIT] dbCurrent = ${currentDir}, dbPrev = ${prevDir}`);

    app.listen(5000, () => {
      console.log("✅ Identity API running on http://localhost:5000");
    });
  } catch (err) {
    console.error("Startup error:", err.message);
  }
})();
