// db.js
const mysql = require("mysql2/promise");

const pool = mysql.createPool({
  host: "localhost",
  user: "root",
  password: "",
  database: "testing_lmdb",
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0,
});

// Direct connection for single queries or transactions
async function getConnection() {
  return await mysql.createConnection({
    host: "localhost",
    user: "root",
    password: "",
    database: "testing_lmdb",
    port: 3306,
  });
}

module.exports = { pool, getConnection };
