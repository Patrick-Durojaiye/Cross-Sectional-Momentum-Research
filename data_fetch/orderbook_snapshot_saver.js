const fs = require('fs');
const path = require('path');
const { streamNormalized, normalizeBookChanges, compute, computeBookSnapshots } = require('tardis-dev');
const { stringify } = require('csv-stringify/sync');

// Path to the directory where the CSV data will be saved
const dataDir = path.join(__dirname, '..', 'data', 'uncleaned', 'orderbook_snapshot_data');
const logFilePath = path.join(__dirname, 'scraper.log');

// Ensure the data directory exists
if (!fs.existsSync(dataDir)) {
    fs.mkdirSync(dataDir, { recursive: true });
}

// Create a writable stream for the log file
const logStream = fs.createWriteStream(logFilePath, { flags: 'a' });

class OrderBookSnapshotSaver {
    constructor(exchange, symbol, filePath) {
        this.exchange = exchange;
        this.symbol = symbol;
        this.filePath = path.join(dataDir, filePath);
        this.stream = null;

        // Write CSV header if the file doesn't exist
        if (!fs.existsSync(this.filePath)) {
            fs.writeFileSync(this.filePath, 'timestamp,localTimestamp,bid_price_0,bid_amount_0,ask_price_0,ask_amount_0\n');
        }
    }

    initializeStream() {
        this.stream = streamNormalized(
            {
                exchange: this.exchange,
                symbols: [this.symbol]
            },
            normalizeBookChanges
        );

        return compute(
            this.stream,
            // 1-minute order book snapshots at 1 level depth
            computeBookSnapshots({ depth: 1, interval: 60 * 1000 })
        );
    }

    async startSavingSnapshots() {
        if (!this.stream) {
            this.logMessage('Stream not initialized. Call initializeStream() before starting.');
            return;
        }

        const messages = this.initializeStream();
        for await (const message of messages) {
            if (message.type === 'book_snapshot') {
                // Extract relevant data from the message
                const data = {
                    timestamp: message.timestamp,
                    localTimestamp: message.localTimestamp,
                    bid_price_0: message.bids[0]?.price || '',
                    bid_amount_0: message.bids[0]?.amount || '',
                    ask_price_0: message.asks[0]?.price || '',
                    ask_amount_0: message.asks[0]?.amount || ''
                };

                // Convert data to CSV format
                const csvRow = stringify([[data.timestamp, data.localTimestamp, data.bid_price_0, data.bid_amount_0, data.ask_price_0, data.ask_amount_0]], { header: false });

                // Append the row to the CSV file
                fs.appendFile(this.filePath, csvRow, (err) => {
                    if (err) {
                        this.logMessage(`Error writing to CSV file: ${err}`);
                    } else {
                        this.logMessage('Processed and saved CSV row');
                    }
                });

            }
        }
    }

    logMessage(message) {
        const timestamp = new Date().toISOString();
        logStream.write(`[${timestamp}] ${message}\n`);
    }
}

// Example usage:
const snapshotSaver = new OrderBookSnapshotSaver('bitmex', 'XBTUSD', 'orderbook_snapshot_data.csv');
snapshotSaver.startSavingSnapshots();
