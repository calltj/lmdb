const express = require("express");
const mysql = require("mysql2/promise");
const axios = require("axios");

const app = express();
app.use(express.json());

let mysqlConn;

const identityAPI = "http://localhost:5000";

// Check email existence
const emailExists = async (email) => {
  const response = await axios.get(`${identityAPI}/check`, {
    params: { email },
  });
  return response.data.exists;
};

// Signup (write to LMDB only)
app.post("/signup", async (req, res) => {
  const { email, name, age } = req.body;
  if (!email || !name || !age)
    return res.status(400).json({ error: "Missing fields." });

  try {
    if (await emailExists(email)) {
      return res.status(409).json({ error: "Email already exists." });
    }

    const userId = `ecommerce-${Date.now().toString(36)}`;
    const user = { userId, email, name, age, app: "ecommerce" };

    try {
      await axios.post(
        `${identityAPI}/identity`,
        { user },
        { headers: { "x-app-name": "ecommerce" } }
      );
    } catch (axiosError) {
      console.error(
        "AXIOS ERROR:",
        axiosError.response?.data || axiosError.message
      );
      return res.status(500).json({
        error: "Request failed",
        details: axiosError.response?.data || axiosError.message,
      });
    }

    res.status(201).json({ message: "User created via cache", user });
  } catch (err) {
    console.error("SIGNUP ERROR:", err);
    res.status(500).json({ error: "Signup failed", details: err.message });
  }
});

// Login
app.post("/login", async (req, res) => {
  const { email } = req.body;
  if (!email) return res.status(400).json({ error: "Email required" });

  try {
    const response = await axios.post(
      `${identityAPI}/auth`,
      { email },
      { headers: { "x-app-name": "ecommerce" } }
    );
    res.json({ user: response.data.user });
  } catch (err) {
    res.status(500).json({ error: "Login failed", details: err.message });
  }
});

// Get profile
app.get("/profile/:email", async (req, res) => {
  try {
    const response = await axios.post(
      `${identityAPI}/identity`,
      { user: { email: req.params.email } },
      { headers: { "x-app-name": "ecommerce" } }
    );
    res.json({ user: response.data.user });
  } catch (err) {
    res
      .status(500)
      .json({ error: "Profile fetch failed", details: err.message });
  }
});

// Update profile (update LMDB cache only)
app.put("/update-profile", async (req, res) => {
  const { userId, name, age } = req.body;
  if (!userId || !name || !age) {
    return res.status(400).json({ error: "Missing fields." });
  }

  try {
    await axios.post(
      `${identityAPI}/identity`,
      { user: { userId, name, age, app: "ecommerce" } },
      { headers: { "x-app-name": "ecommerce" } }
    );

    res.json({ message: "Profile updated in LMDB" });
  } catch (err) {
    res.status(500).json({ error: "Update failed", details: err.message });
  }
});

// Check balance
app.get("/check-balance/:email", async (req, res) => {
  const { email } = req.params;

  try {
    const [rows] = await mysqlConn.execute(
      "SELECT balance FROM users WHERE email = ?",
      [email]
    );

    if (rows.length === 0) {
      return res.status(404).json({ error: "User not found" });
    }

    res.json({ email, balance: rows[0].balance });
  } catch (err) {
    console.error("BALANCE CHECK ERROR:", err);
    res
      .status(500)
      .json({ error: "Balance check failed", details: err.message });
  }
});

// Add to cart
app.post("/add-to-cart", async (req, res) => {
  const { userId, productId, quantity } = req.body;
  if (!userId || !productId || !quantity)
    return res.status(400).json({ error: "Missing fields." });

  try {
    await mysqlConn.execute(
      "INSERT INTO cart (userId, productId, quantity) VALUES (?, ?, ?)",
      [userId, productId, quantity]
    );
    res.json({ message: "Added to cart" });
  } catch (err) {
    res.status(500).json({ error: "Add to cart failed", details: err.message });
  }
});

// Checkout
app.post("/checkout", async (req, res) => {
  const { userId, total } = req.body;

  try {
    const [rows] = await mysqlConn.execute(
      "SELECT balance FROM users WHERE userId = ?",
      [userId]
    );

    if (rows.length === 0) {
      return res.status(404).json({ error: "User not found" });
    }

    if (rows[0].balance < total) {
      return res.status(400).json({ error: "Insufficient balance" });
    }

    await mysqlConn.execute(
      "UPDATE users SET balance = balance - ? WHERE userId = ?",
      [total, userId]
    );

    await mysqlConn.execute("DELETE FROM cart WHERE userId = ?", [userId]);

    res.json({ message: "Checkout complete" });
  } catch (err) {
    res.status(500).json({ error: "Checkout failed", details: err.message });
  }
});

// Start server
(async () => {
  try {
    mysqlConn = await mysql.createConnection({
      host: "127.0.0.1",
      user: "root",
      password: "",
      database: "testing_lmdb",
      port: 3306,
    });

    console.log("[✅] MySQL connected on ecommerce backend");
    app.listen(6000, () => {
      console.log("🛒 E-Commerce backend running at http://localhost:6000");
    });
  } catch (err) {
    console.error("[❌] Failed to connect to MySQL:", err.message);
  }
})();
