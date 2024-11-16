const fs = require('fs');
const path = require('path');
require('dotenv').config();
const { init, replayNormalized, normalizeBookChanges, compute, computeBookSnapshots, normalizeTrades } = require('tardis-dev');
const { stringify } = require('csv-stringify/sync');

const logFilePath = path.join(__dirname, 'order_book_snapshot_saver.log');

init({
    apiKey: process.env.TARDIS_API_KEY
});

// Create a writable stream for the log file
const logStream = fs.createWriteStream(logFilePath, { flags: 'a' });

class OrderBookSnapshotSaver {
    constructor(exchange, symbol, filePath, dataDir, start_date) {
        this.start_date = start_date;
        this.exchange = exchange;
        this.symbol = symbol;
        this.filePath = path.join(dataDir, filePath);
        this.stream = null;
        this.to_date = '2019-11-18';

        // Write CSV header if the file doesn't exist
        if (!fs.existsSync(this.filePath)) {
            fs.writeFileSync(this.filePath, 'timestamp,localTimestamp,bid_price_0,bid_amount_0,ask_price_0,ask_amount_0\n');
        }
    }

    initializeStream() {
        this.stream = replayNormalized(
            {
                exchange: this.exchange,
                symbols: [this.symbol],
                from: this.start_date,
                to: this.to_date
            },
            normalizeBookChanges
        );
        return this.stream;
    }

    async startSavingSnapshots() {
        const messages = this.initializeStream();
        console.log(this.symbol);
        console.log('from date', this.start_date);
        console.log('to date', this.to_date);
        console.log('exchange', this.exchange);

        if (!messages) {
            this.logMessage('Stream not initialized. Call initializeStream() before starting.');
            return;
        }

        // Apply the compute function to generate book snapshots
        const messagesWithComputedTypes = compute(
            messages,
            computeBookSnapshots({ depth: 1, interval: 60 * 1000 }) // 1 depth and 60-second interval
        );

        for await (const message of messagesWithComputedTypes) {
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

module.exports = { OrderBookSnapshotSaver };
