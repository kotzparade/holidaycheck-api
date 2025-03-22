import { BigQuery } from "@google-cloud/bigquery";
import axios from "axios";

export class HotelReviewService {
  constructor(config, hotel) {
    this.config = config;
    this.hotel = hotel;
    this.bigquery = new BigQuery({
      projectId: config.projectId,
    });
  }

  parseChildren(childrenString) {
    const mapping = {
      NO: 0,
      ONE: 1,
      TWO: 2,
      THREE: 3,
      FOUR: 4,
      MORE: 5,
    };
    return mapping[childrenString] || 0;
  }

  validateReview(review) {
    if (!review?.id || !review?.title || !review?.user?.id) {
      console.log(
        `[${this.hotel.name}] Invalid review data:`,
        JSON.stringify(review)
      );
      return false;
    }
    return true;
  }

  async getExistingReviewIds() {
    const query = `
      SELECT review_id 
      FROM \`${this.config.projectId}.${this.config.datasetId}.${this.hotel.tableId}\`
    `;

    try {
      const [rows] = await this.bigquery.query({
        query,
        location: "US",
      });

      const existingReviewIds = rows.map((row) => row.review_id);
      console.log(
        `[${this.hotel.name}] Fetched ${existingReviewIds.length} existing review IDs`
      );
      return existingReviewIds;
    } catch (error) {
      console.error(
        `[${this.hotel.name}] Error fetching existing review IDs:`,
        error
      );
      return [];
    }
  }

  async updateTotalCount(totalReviews) {
    try {
      // Instead of updating existing rows, we'll insert a new row with the total count
      const query = `
        INSERT INTO \`${this.config.projectId}.${this.config.datasetId}.${
        this.hotel.tableId
      }\`
        (review_id, title, general_text, entry_date, total_reviews)
        VALUES
        ('total_count_${Date.now()}', 'Total Reviews Count', '', CURRENT_TIMESTAMP(), @total_reviews)
      `;

      const options = {
        query,
        location: "US",
        params: {
          total_reviews: totalReviews,
        },
      };

      await this.bigquery.query(options);
      console.log(
        `[${this.hotel.name}] Added total review count: ${totalReviews}`
      );
    } catch (error) {
      console.error(`[${this.hotel.name}] Error adding total count:`, error);
    }
  }

  async getLatestTotalCount() {
    try {
      const query = `
        SELECT total_reviews
        FROM \`${this.config.projectId}.${this.config.datasetId}.${this.hotel.tableId}\`
        WHERE review_id LIKE 'total_count_%'
        ORDER BY entry_date DESC
        LIMIT 1
      `;

      const [rows] = await this.bigquery.query({
        query,
        location: "US",
      });

      return rows[0]?.total_reviews || null;
    } catch (error) {
      console.error(
        `[${this.hotel.name}] Error getting latest total count:`,
        error
      );
      return null;
    }
  }

  async insertNewReviews(newReviews) {
    if (!newReviews?.length) {
      console.log(`[${this.hotel.name}] No new reviews to insert`);
      return;
    }

    console.log(`[${this.hotel.name}] Processing ${newReviews.length} reviews`);

    const rows = newReviews
      .filter((review) => this.validateReview(review))
      .map((review) => ({
        review_id: String(review.id),
        title: review.title,
        general_text: review.texts?.GENERAL || "",
        rating_general: review.ratings?.GENERAL?.GENERAL || null,
        travel_date: new Date(review.travelDate).toISOString(),
        entry_date: new Date(review.entryDate).toISOString(),
        user_id: String(review.user.id),
        travel_reason: review.travelReason,
        traveled_with: review.traveledWith,
        children: this.parseChildren(review.children),
        total_reviews: null, // This will be updated later
      }));

    if (!rows.length) {
      console.log(`[${this.hotel.name}] No valid rows after filtering`);
      return;
    }

    try {
      await this.bigquery
        .dataset(this.config.datasetId)
        .table(this.hotel.tableId)
        .insert(rows);
      console.log(
        `[${this.hotel.name}] Successfully inserted ${rows.length} reviews`
      );
    } catch (error) {
      console.error(`[${this.hotel.name}] Failed to insert reviews:`, error);
      throw error;
    }
  }

  async fetchNewReviews() {
    const { api } = this.config;
    let offset = 0;
    let retryCount = 0;
    let retryDelay = api.initialRetryDelay;
    const newReviews = [];
    let hasMoreReviews = true;
    let totalReviews = 0;

    // Get existing review IDs
    const existingReviewIds = await this.getExistingReviewIds();

    // Get the most recent review's entry date from BigQuery
    const [latestReviews] = await this.bigquery.query({
      query: `
        SELECT entry_date
        FROM \`${this.config.projectId}.${this.config.datasetId}.${this.hotel.tableId}\`
        ORDER BY entry_date DESC
        LIMIT 1
      `,
      location: "US",
    });

    const latestReviewDate = latestReviews[0]?.entry_date;
    console.log(
      `[${this.hotel.name}] Latest review date in database: ${
        latestReviewDate || "None"
      }`
    );

    while (hasMoreReviews && offset < api.maxOffset) {
      const queryParams = {
        select:
          "id,title,texts,ratings,travelDate,entryDate,user,travelReason,traveledWith,children",
        filter: `hotel.id:${this.hotel.id}`,
        sort: "entryDate:desc",
        limit: api.batchSize,
        offset,
        locale: api.defaultLocale,
      };

      try {
        const response = await axios.get(`${api.baseUrl}/hotelreview`, {
          params: queryParams,
          headers: {
            "Partner-ID": this.hotel.partnerId,
          },
        });

        const { data } = response;

        // Store total reviews count from first request
        if (offset === 0) {
          totalReviews = data.total;
          await this.updateTotalCount(totalReviews);
        }

        if (data.items?.length > 0) {
          // Check if all reviews in this batch are older than our latest review
          if (latestReviewDate) {
            const allReviewsOlder = data.items.every(
              (item) => new Date(item.entryDate) <= new Date(latestReviewDate)
            );

            if (allReviewsOlder) {
              console.log(
                `[${this.hotel.name}] All reviews in batch are older than latest stored review. Stopping fetch.`
              );
              hasMoreReviews = false;
              break;
            }
          }

          const newItems = data.items.filter(
            (item) => !existingReviewIds.includes(String(item.id))
          );

          if (newItems.length > 0) {
            newReviews.push(...newItems);
            console.log(
              `[${this.hotel.name}] Fetched ${data.items.length} reviews, ${newItems.length} are new. Total: ${newReviews.length}`
            );
            offset += api.batchSize;
          } else {
            console.log(
              `[${this.hotel.name}] No new reviews in this batch. Continuing to check next batch...`
            );
            offset += api.batchSize;
          }
          retryCount = 0;
        } else {
          hasMoreReviews = false;
          console.log(
            `[${this.hotel.name}] No more reviews available from API.`
          );
        }

        await new Promise((resolve) => setTimeout(resolve, 1000));
      } catch (error) {
        console.error(
          `[${this.hotel.name}] Error fetching reviews:`,
          error.message
        );

        if (error.response?.status === 429 && retryCount < api.maxRetries) {
          retryCount++;
          console.log(
            `[${this.hotel.name}] Rate limit exceeded. Retrying in ${
              retryDelay / 1000
            }s. Retry ${retryCount}/${api.maxRetries}`
          );
          await new Promise((resolve) => setTimeout(resolve, retryDelay));
          retryDelay *= 2;
        } else {
          hasMoreReviews = false;
        }
      }
    }

    if (newReviews.length > 0) {
      await this.insertNewReviews(newReviews);
    } else {
      console.log(`[${this.hotel.name}] No new reviews found to insert.`);
    }

    return newReviews.length;
  }
}
