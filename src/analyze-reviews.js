import { config } from "./config/index.js";
import { ReviewAnalysisService } from "./services/ReviewAnalysisService.js";
import dotenv from "dotenv";

dotenv.config();

async function analyzeReviews(days = 30) {
  try {
    console.log(`Starting review analysis for the last ${days} days...`);

    for (const hotel of config.hotels) {
      console.log(`\nProcessing hotel: ${hotel.name}`);
      const analysisService = new ReviewAnalysisService(config, hotel);
      await analysisService.analyzeRecentReviews(days);
    }

    console.log("\nReview analysis completed successfully");
  } catch (error) {
    console.error("Error during review analysis:", error);
    process.exit(1);
  }
}

// Get days from command line argument or use default
const days = parseInt(process.argv[2]) || 30;
analyzeReviews(days);
