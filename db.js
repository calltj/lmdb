// db.js
const mysql = require("mysql2/promise");
require
const pool = mysql.createPool({
  host: process.env.DB_HOST,
      user: process.env.DB_USER,
      password: process.env.DB_PASSWORD,
      database: process.env.DB_NAME,
      port: process.env.DB_PORT,
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0,
});

// Direct connection for single queries or transactions
async function getConnection() {
  return await mysql.createConnection({
   host: process.env.DB_HOST,
      user: process.env.DB_USER,
      password: process.env.DB_PASSWORD,
      database: process.env.DB_NAME,
    port: process.env.DB_PORT,
  });
}

module.exports = { pool, getConnection };
