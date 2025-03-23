import { HotelReviewService } from "./services/HotelReviewService.js";
import { ReviewAnalysisService } from "./services/ReviewAnalysisService.js";
import { config } from "./config/index.js";
import { hotels } from "./config/hotels.js";
import axios from "axios";

async function runDailyUpdate() {
  const today = new Date();
  const isFirstOfMonth = today.getDate() === 1;

  console.log(`Starting daily update - ${today.toISOString()}`);
  console.log(`Is first of month: ${isFirstOfMonth}`);

  for (const hotel of hotels) {
    try {
      console.log(`\nProcessing hotel: ${hotel.name}`);

      // Always run the review update
      const reviewService = new HotelReviewService(config, hotel);

      // First, get the current total from the API
      const response = await axios.get(`${config.api.baseUrl}/hotelreview`, {
        params: {
          select: "id",
          filter: `hotel.id:${hotel.id}`,
          limit: 1,
          offset: 0,
          locale: config.api.defaultLocale,
        },
        headers: {
          "Partner-ID": hotel.partnerId,
        },
      });

      // Update the total count
      await reviewService.updateTotalCount(response.data.total);
      console.log(
        `[${hotel.name}] Updated total review count: ${response.data.total}`
      );

      // Then fetch any new reviews
      const newReviewsCount = await reviewService.fetchNewReviews();
      console.log(
        `[${hotel.name}] Updated reviews: ${newReviewsCount} new reviews`
      );

      // Get the latest total count from ReviewTotals
      const latestTotal = await reviewService.getLatestTotalCount();
      console.log(`[${hotel.name}] Current total reviews: ${latestTotal}`);

      // Only run analysis on the first day of each month
      if (isFirstOfMonth) {
        console.log(`[${hotel.name}] Running monthly analysis...`);
        const analysisService = new ReviewAnalysisService(config, hotel);
        await analysisService.analyzeRecentReviews(30); // Analyze last 30 days
        console.log(`[${hotel.name}] Monthly analysis completed`);
      } else {
        console.log(`[${hotel.name}] Skipping analysis (not first of month)`);
      }
    } catch (error) {
      console.error(`Error processing hotel ${hotel.name}:`, error);
    }
  }

  console.log("\nDaily update completed");
}

// Run the update
runDailyUpdate().catch(console.error);
