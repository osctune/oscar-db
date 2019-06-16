const MongoClient = require('mongodb').MongoClient;

let indexCreated = false;
const createIndex = async (db) => {
    // Create index for id collection.
    const ids = db.collection('id');
    await ids.createIndex({
        value: 1,
    });

    // Crate index for hash collection.
    const hashes = db.collection('hash');
    
    // hash field.
    await hashes.createIndex({
        hash: 1,
    }, {
        unique: true, // Should be unique due to atomic operation for generating id.
    });

    // url field.
    await hashes.createIndex({
        url: 1,
    }, {
        unique: false, // Not unique in case of multiple requests for same url (concurrency problem).
    });
};

// Connect to db and call callback when ready, cleanup when done.
const mongo = (mongoUri, dbName) => async callback => {
    let connection;
    try {
        // Connect to db.
        connection = await MongoClient.connect(mongoUri, { useNewUrlParser: true });
        
        // Get db by name.
        const db = connection.db(dbName);

        // Ensure indexed.
        if(!indexCreated) {
            await createIndex(db);
            indexCreated = true;
        }

        // Do work.
        return await callback(db);
    } catch(err) {
        console.log(err);
    } finally {
        // Finally close connection.
        connection && await connection.close();
    }
};

// Get next unique id from db.
const createId = async (db) => {
    const collection = db.collection('id');
    const result = await collection.findOneAndUpdate({}, {
        $inc: {
            value: 1, // Increse value by one.
        },
    }, {
        upsert: true,   // Create if no document.
        projection: {
            _id: 0,     // We don't need the _id.
            value: 1,   // Include the value.
        },
    });
    return result.value ? result.value.value : null;
};

// Add hash to db.
const addHash = async (db, url, hash) => {
    const collection = db.collection('hash');
    await collection.insertOne({
        url,
        hash,
    });
};

// Find hash in db by url.
const findHash = async (db, url) => {
    const collection = db.collection('hash');
    const result = await collection.findOne({
        url,
    }, {
        projection: {
            _id: 0,     // We don't need the _id.
            hash: 1,    // Include the hash.
        },
    });
    return result ? result.hash : null;
};

// Find url in db by hash.
const findUrl = async (db, hash) => {
    const collection = db.collection('hash');
    const result = await collection.findOne({
        hash,
    }, {
        projection: {
            _id: 0,     // We don't need the _id.
            url: 1,    // Include the hash.
        },
    });
    return result ? result.url : null;
};

module.exports = {
    createIndex,
    mongo,
    createId,
    addHash,
    findHash,
    findUrl,
};