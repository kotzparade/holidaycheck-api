import { ReviewAnalysisService } from "./services/ReviewAnalysisService.js";
import { config } from "./config/index.js";
import { hotels } from "./config/hotels.js";

async function runAnalysis(hotelName = null, days = 30) {
  console.log(`Starting manual analysis - ${new Date().toISOString()}`);
  console.log(`Analysis period: last ${days} days`);

  const hotelsToAnalyze = hotelName
    ? hotels.filter((h) => h.name === hotelName)
    : hotels;

  if (hotelsToAnalyze.length === 0) {
    console.error(
      `No hotels found${hotelName ? ` with name "${hotelName}"` : ""}`
    );
    return;
  }

  for (const hotel of hotelsToAnalyze) {
    try {
      console.log(`\nProcessing hotel: ${hotel.name}`);
      const analysisService = new ReviewAnalysisService(config, hotel);
      await analysisService.analyzeRecentReviews(days);
      console.log(`[${hotel.name}] Analysis completed successfully`);
    } catch (error) {
      console.error(`Error analyzing hotel ${hotel.name}:`, error);
    }
  }

  console.log("\nAnalysis completed");
}

// Get command line arguments
const args = process.argv.slice(2);
const hotelName = args[0]; // Optional hotel name
const days = parseInt(args[1]) || 30; // Optional number of days

// Run the analysis
runAnalysis(hotelName, days).catch(console.error);
