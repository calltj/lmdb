// auth.js

const { ObjectId } = require('mongodb');

// 🔎 LMDB-first + fallback lookup
async function loginOrRecovery(email, { db, mongoDbs, mysqlConn }, source) {
  const cached = db.get(`email:${email}`);
  if (cached) {
    console.log('✅ Found in LMDB');
    return cached;
  }

  // 🔍 Search MongoDB
  if (source === 'mongodb') {
    for (let i = 0; i < mongoDbs.length; i++) {
      const user = await mongoDbs[i].findOne({ email });
      if (user) {
        await db.put(`email:${email}`, user); // Cache
        console.log(`📥 Found in MongoDB db${i + 1}`);
        return user;
      }
    }
    console.log('❌ Not found in MongoDB');
  }

  // 🔍 Search MySQL
  if (source === 'mysql') {
    const [rows] = await mysqlConn.query('SELECT * FROM users WHERE email = ?', [email]);
    if (rows.length > 0) {
      const user = rows[0];
      await db.put(`email:${email}`, user); // Cache
      console.log('📥 Found in MySQL');
      return user;
    }
    console.log('❌ Not found in MySQL');
  }

  console.log('⛔ No user found.');
  return null;
}


async function signupUser({ email, name, age, source }, { db, mongoDbs, mysqlConn }) {
  // Step 1: Check LMDB cache
  const existingLMDB = db.get(`email:${email}`);
  if (existingLMDB) {
    console.log('❌ Signup rejected: email already exists in LMDB');
    return null;
  }

  // Step 2: Check MongoDB across all databases
  for (let i = 0; i < mongoDbs.length; i++) {
    const mongoUser = await mongoDbs[i].findOne({ email });
    if (mongoUser) {
      console.log(`❌ Signup rejected: email already exists in MongoDB db${i + 1}`);
      return null;
    }
  }

  // Step 3: Check MySQL
  const [rows] = await mysqlConn.query('SELECT * FROM users WHERE email = ?', [email]);
  if (rows.length > 0) {
    console.log('❌ Signup rejected: email already exists in MySQL');
    return null;
  }

  // Step 4: Create and insert new user
  const userId = new ObjectId().toString();
  const newUser = {
    userId,
    name,
    email,
    age
  };

  if (source === 'mongodb') {
    const result = await mongoDbs[0].insertOne(newUser);
    console.log(`✅ Signed up in MongoDB with _id: ${result.insertedId}`);
    return { ...newUser, _id: result.insertedId };
  }

  if (source === 'mysql') {
    await mysqlConn.query(
      'INSERT INTO users (userId, name, email, age) VALUES (?, ?, ?, ?)',
      [userId, name, email, age]
    );
    console.log(`✅ Signed up in MySQL with userId: ${userId}`);
    return newUser;
  }

  console.log('❌ Invalid source for signup');
  return null;
}

module.exports = {
  loginOrRecovery,
  signupUser
};