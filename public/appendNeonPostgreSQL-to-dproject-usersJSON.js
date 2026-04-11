// appendNeonPostgreSQL-to-dproject-usersJSON.js

require('dotenv').config();

const fs = require('fs');
const { Client } = require('pg');

// File paths - now we only need the source JSON file
const TARGET_JSON_PATH = './dproject-users.json';

// Check if DATABASE_URL is set
if (!process.env.DATABASE_URL) {
  console.error('❌ DATABASE_URL environment variable is not set');
  console.log('\n💡 How to set DATABASE_URL:');
  console.log('   1. Create a .env file with: DATABASE_URL="your-connection-string"');
  console.log('   2. Or run: export DATABASE_URL="your-connection-string"');
  console.log('   3. Or set it in your deployment environment');
  console.log('\n🔗 Your Neon PostgreSQL connection string should look like:');
  console.log('   postgresql://username:password@host:port/database');
  process.exit(1);
}

const dbClient = new Client({
  connectionString: process.env.DATABASE_URL,
  ssl: false
});

// Function to connect to Neon PostgreSQL database
async function connectToDatabase() {
  try {
    await dbClient.connect();
    console.log('✅ Connected to Neon PostgreSQL database');
    return dbClient;
  } catch (error) {
    console.error('❌ Database connection failed:', error.message);
    
    // Try alternative connection without SSL
    console.log('🔄 Attempting connection without SSL...');
    const fallbackClient = new Client({
      connectionString: process.env.DATABASE_URL,
      ssl: false
    });
    
    await fallbackClient.connect();
    console.log('✅ Connected to database without SSL');
    return fallbackClient;
  }
}

// Function to fetch users from PostgreSQL database
async function fetchUsersFromDatabase(client) {
  try {
    const query = `
      SELECT 
        user_id,
        referrer_id,
        email,
        name,
        token_id,
        plan_a
      FROM users 
      ORDER BY CAST(token_id AS INTEGER)
    `;
    
    const result = await client.query(query);
    console.log(`✅ Fetched ${result.rows.length} records from PostgreSQL database`);
    return result.rows;
  } catch (error) {
    console.error('❌ Error fetching data from database:', error.message);
    throw error;
  }
}

// Function to read JSON file
function readJSONFile(filePath) {
  try {
    const data = fs.readFileSync(filePath, 'utf8');
    const jsonData = JSON.parse(data);
    console.log(`✅ Read ${jsonData.length} records from JSON file`);
    return jsonData;
  } catch (error) {
    console.error('❌ Error reading JSON file:', error.message);
    return [];
  }
}

// Function to write JSON file (now writes back to source file)
function writeJSONFile(filePath, data) {
  try {
    // Create backup of original file before modifying
    const backupPath = `${filePath}.backup-${Date.now()}`;
    if (fs.existsSync(filePath)) {
      fs.copyFileSync(filePath, backupPath);
      console.log(`📁 Backup created: ${backupPath}`);
    }
    
    fs.writeFileSync(filePath, JSON.stringify(data, null, 2));
    console.log(`✅ Successfully wrote ${data.length} records to ${filePath}`);
  } catch (error) {
    console.error('❌ Error writing JSON file:', error.message);
  }
}

// Function to convert database record to match dproject-users.json format
function convertDBRecordToDProjectUsersFormat(dbRecord) {
  const { user_id, referrer_id, email, name, token_id, plan_a } = dbRecord;
  
  // Extract date from plan_a JSONB field
  let formattedDate = '';
  if (plan_a && plan_a.dateTime) {
    // Convert date format from "09/11/2025 08:04:03" to "09/11/2025, 08:04:03"
    const [datePart, timePart] = plan_a.dateTime.split(' ');
    formattedDate = `${datePart}, ${timePart}`;
  }
  
  return {
    userId: user_id || '',
    referrerId: referrer_id && referrer_id !== 'None' ? referrer_id : '',
    email: email && email !== 'N/A' ? email : null,
    name: name && name !== 'N/A' ? name : null,
    tokenId: token_id || '',
    planA: formattedDate || ''
  };
}

// Main function
async function main() {
  let client = dbClient;
  let usedFallback = false;

  try {
    console.log('🚀 Starting PostgreSQL to dproject-users JSON append process...\n');
    
    console.log('🔗 Attempting database connection...');
    
    // Connect to database
    try {
      await client.connect();
      console.log('✅ Connected to Neon PostgreSQL database');
    } catch (sslError) {
      if (sslError.message.includes('SSL') || sslError.message.includes('ssl')) {
        console.log('🔄 SSL connection failed, trying without SSL...');
        // Create new client without SSL
        client = new Client({
          connectionString: process.env.DATABASE_URL,
          ssl: false
        });
        await client.connect();
        usedFallback = true;
        console.log('✅ Connected to database without SSL');
      } else {
        throw sslError;
      }
    }
    
    // Read both data sources
    const dbRecords = await fetchUsersFromDatabase(client);
    const jsonRecords = readJSONFile(TARGET_JSON_PATH);
    
    if (jsonRecords.length === 0) {
      console.log('❌ No records found in target JSON file or file is empty');
      return;
    }
    
    // Find the highest tokenId in the existing JSON records
    const highestTokenId = Math.max(...jsonRecords.map(record => parseInt(record.tokenId)));
    console.log(`📊 Highest Token ID in target JSON: ${highestTokenId}`);
    
    // Filter database records to find records with tokenId greater than the highest in JSON
    const newRecords = dbRecords
      .filter(dbRecord => {
        const dbTokenId = parseInt(dbRecord.token_id);
        return !isNaN(dbTokenId) && dbTokenId > highestTokenId;
      })
      .map(convertDBRecordToDProjectUsersFormat);
    
    console.log(`📈 Found ${newRecords.length} new records to append`);
    
    if (newRecords.length === 0) {
      console.log('✅ No new records to append. All records are already in the target JSON file.');
      return;
    }
    
    // Sort new records by tokenId (ascending)
    newRecords.sort((a, b) => parseInt(a.tokenId) - parseInt(b.tokenId));
    
    // Display the new records that will be added
    console.log('\n📋 New records to be appended:');
    newRecords.forEach(record => {
      console.log(`   - Token ID: ${record.tokenId}, User: ${record.name || 'N/A'}, Date: ${record.planA}`);
    });
    
    // Combine existing records with new records
    const combinedRecords = [...jsonRecords, ...newRecords];
    
    // Sort combined records by tokenId (ascending)
    combinedRecords.sort((a, b) => parseInt(a.tokenId) - parseInt(b.tokenId));
    
    // Write the result back to the SOURCE file (not a new file)
    writeJSONFile(TARGET_JSON_PATH, combinedRecords);
    
    console.log('\n🎉 Process completed successfully!');
    console.log(`📁 Original records: ${jsonRecords.length}`);
    console.log(`📁 New records added: ${newRecords.length}`);
    console.log(`📁 Total records in source file: ${combinedRecords.length}`);
    console.log(`📄 Source file updated: ${TARGET_JSON_PATH}`);
    if (usedFallback) {
      console.log('ℹ️  Connected without SSL');
    }
    
  } catch (error) {
    console.error('❌ Error in main process:', error.message);
  } finally {
    // Close database connection
    if (client) {
      await client.end();
      console.log('✅ Database connection closed');
    }
  }
}

// Run the script
main();