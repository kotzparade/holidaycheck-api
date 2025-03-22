import { config } from "./config/index.js";
import { HotelReviewService } from "./services/HotelReviewService.js";

async function runDailyImport() {
  console.log("Starting daily import for all hotels...");
  const results = [];

  for (const hotel of config.hotels) {
    try {
      console.log(`\nProcessing hotel: ${hotel.name}`);
      const service = new HotelReviewService(config, hotel);
      const newReviewsCount = await service.fetchNewReviews();
      results.push({
        hotel: hotel.name,
        newReviews: newReviewsCount,
        status: "success",
      });
    } catch (error) {
      console.error(`Error processing hotel ${hotel.name}:`, error);
      results.push({
        hotel: hotel.name,
        newReviews: 0,
        status: "error",
        error: error.message,
      });
    }
  }

  // Print summary
  console.log("\n=== Import Summary ===");
  for (const result of results) {
    const status = result.status === "success" ? "✓" : "✗";
    const message =
      result.status === "success"
        ? `${result.newReviews} new reviews imported`
        : `Failed: ${result.error}`;
    console.log(`${status} ${result.hotel}: ${message}`);
  }
}

// Run the import
runDailyImport().catch((error) => {
  console.error("Fatal error during import:", error);
  process.exit(1);
});
