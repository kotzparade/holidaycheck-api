import { config } from "./config/index.js";
import { ReviewAnalysisService } from "./services/ReviewAnalysisService.js";
import dotenv from "dotenv";

dotenv.config();

/**
 * Analyzes reviews for specified hotels
 * @param {Object} options - Analysis options
 * @param {string} [options.hotelName] - Specific hotel name to analyze
 * @param {number} [options.reviewCount] - Number of reviews to analyze (default: 30)
 * @param {number} [options.days] - Number of days to look back (default: null)
 * @param {boolean} [options.byCount=true] - Whether to analyze by review count or by days
 */
async function analyzeReviews(options = {}) {
  const {
    hotelName = null,
    reviewCount = 30,
    days = null,
    byCount = true,
  } = options;

  try {
    console.log(`Starting review analysis - ${new Date().toISOString()}`);
    console.log(`Analysis mode: ${byCount ? "By review count" : "By days"}`);
    console.log(
      `Parameters: ${byCount ? `${reviewCount} reviews` : `${days} days`}`
    );
    if (hotelName) {
      console.log(`Hotel filter: ${hotelName}`);
    }

    // Filter hotels if hotelName is provided
    const hotelsToAnalyze = hotelName
      ? config.hotels.filter((h) => h.name === hotelName)
      : config.hotels;

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

        if (byCount) {
          await analysisService.analyzeRecentReviewsByCount(reviewCount);
        } else {
          await analysisService.analyzeRecentReviews(days);
        }

        console.log(`[${hotel.name}] Analysis completed successfully`);
      } catch (error) {
        console.error(`Error analyzing hotel ${hotel.name}:`, error);
        // Continue with other hotels even if one fails
      }
    }

    console.log("\nReview analysis completed");
  } catch (error) {
    console.error("Error during review analysis:", error);
    process.exit(1);
  }
}

// Parse command line arguments
const args = process.argv.slice(2);
const options = {
  hotelName: null,
  reviewCount: 30,
  days: null,
  byCount: true,
};

// Parse arguments
for (let i = 0; i < args.length; i++) {
  const arg = args[i];

  if (arg === "--hotel" || arg === "-h") {
    options.hotelName = args[++i];
  } else if (arg === "--count" || arg === "-c") {
    options.reviewCount = parseInt(args[++i]);
    options.byCount = true;
  } else if (arg === "--days" || arg === "-d") {
    options.days = parseInt(args[++i]);
    options.byCount = false;
  }
}

// Run the analysis
analyzeReviews(options);
