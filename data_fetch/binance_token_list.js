const { getExchangeDetails } = require('tardis-dev');
const fs = require('fs');
const path = require('path');

// Function to get available symbols from Binance and write to a CSV file
async function checkBinanceSymbolAvailability() {
    try {
        const binanceExchangeDetails = await getExchangeDetails('binance-futures');

        const availableSymbols = binanceExchangeDetails.availableSymbols.map(symbol => ({
            id: symbol.id,
            type: symbol.type,
            availableSince: symbol.availableSince
        }));

        // Create a CSV header
        const csvHeader = 'id,type,availableSince\n';

        // Map the availableSymbols data to CSV format
        const csvRows = availableSymbols.map(symbol =>
            `${symbol.id},${symbol.type},${symbol.availableSince}`
        ).join('\n');

        // Complete CSV content
        const csvContent = csvHeader + csvRows;

        // Specify the output path
        const outputDir = path.join(__dirname, '../data/uncleaned/binance_token_list');
        const filePath = path.join(outputDir, 'binance_token_list.csv');

        // Ensure the directory exists
        if (!fs.existsSync(outputDir)) {
            fs.mkdirSync(outputDir, { recursive: true });
        }

        // Write the CSV content to a file
        fs.writeFileSync(filePath, csvContent);

        console.log(`CSV file written successfully to ${filePath}`);
    } catch (error) {
        console.error('Error fetching Binance exchange details:', error);
    }
}

// Example usage to check available symbols and write to CSV
checkBinanceSymbolAvailability();
