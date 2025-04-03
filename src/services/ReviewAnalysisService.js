import { BigQuery } from "@google-cloud/bigquery";
import OpenAI from "openai";
import dotenv from "dotenv";

dotenv.config();

export class ReviewAnalysisService {
  constructor(config, hotel) {
    this.config = config;
    this.hotel = hotel;
    this.bigquery = new BigQuery({
      projectId: config.projectId,
      keyFilename: config.keyFilename,
    });
    this.openai = new OpenAI({
      apiKey: process.env.OPENAI_API_KEY,
    });
  }

  async getLatestTotalCount() {
    try {
      const [rows] = await this.bigquery.query({
        query: `
          SELECT total_reviews
          FROM \`${this.config.projectId}.${this.config.datasetId}.ReviewTotals\`
          WHERE hotel_id = @hotel_id
          ORDER BY last_updated DESC
          LIMIT 1
        `,
        params: {
          hotel_id: this.hotel.id,
        },
      });

      if (rows && rows.length > 0) {
        return rows[0].total_reviews;
      }
      return null;
    } catch (error) {
      console.error(
        `[${this.hotel.name}] Error getting latest total count:`,
        error
      );
      return null;
    }
  }

  async analyzeRecentReviews(days = 30) {
    try {
      // Ensure analysis table exists with correct schema
      await this.ensureAnalysisTableExists();

      // Get the latest total review count
      const totalCount = await this.getLatestTotalCount();
      if (!totalCount) {
        throw new Error("Could not get total review count");
      }

      // Get recent reviews that haven't been analyzed yet
      const [reviews] = await this.bigquery.query({
        query: `
          SELECT *
          FROM \`${this.config.projectId}.${this.config.datasetId}.${this.hotel.tableId}\`
          WHERE DATE(entry_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL ${days} DAY)
          AND review_id NOT IN (
            SELECT review_id 
            FROM \`${this.config.projectId}.${this.config.datasetId}.AnalyzedReviewIDs\`
          )
          ORDER BY entry_date DESC
          LIMIT 50
        `,
      });

      if (reviews.length === 0) {
        console.log(`[${this.hotel.name}] No new reviews to analyze`);
        return;
      }

      console.log(
        `[${this.hotel.name}] Analyzing ${reviews.length} reviews from the last ${days} days`
      );

      // Process reviews in batches of 10
      const batchSize = 10;
      const batches = [];
      for (let i = 0; i < reviews.length; i += batchSize) {
        batches.push(reviews.slice(i, i + batchSize));
      }

      let allResults = {
        overall_sentiment: "",
        positive_points: [],
        negative_points: [],
        common_themes: [],
        areas_for_improvement: [],
      };

      // Process each batch
      for (let i = 0; i < batches.length; i++) {
        const batch = batches[i];
        console.log(
          `[${this.hotel.name}] Processing batch ${i + 1}/${batches.length} (${
            batch.length
          } reviews)`
        );

        const reviewsText = batch
          .map(
            (r) =>
              `Bewertung: ${r.rating_general}/6\nTitel: ${r.title}\nKommentar: ${r.general_text}\n`
          )
          .join("\n");

        const response = await this.openai.chat.completions.create({
          model: "gpt-4",
          messages: [
            {
              role: "system",
              content: `Du bist ein Experte für Hotelbewertungen. Analysiere die folgenden Bewertungen und erstelle eine Zusammenfassung im folgenden JSON-Format (ohne Markdown-Formatierung):
{
  "overall_sentiment": "Gesamteindruck der Bewertungen",
  "positive_points": ["Positivpunkt 1", "Positivpunkt 2", ...],
  "negative_points": ["Negativpunkt 1", "Negativpunkt 2", ...],
  "common_themes": ["Hauptthema 1", "Hauptthema 2", ...],
  "areas_for_improvement": ["Verbesserungsbereich 1", "Verbesserungsbereich 2", ...]
}`,
            },
            {
              role: "user",
              content: `Analysiere diese Hotelbewertungen:\n\n${reviewsText}`,
            },
          ],
          temperature: 0.7,
        });

        const content = response.choices[0].message.content;

        // Clean the response - remove markdown formatting if present
        const cleanContent = content.replace(/```json\n?|\n?```/g, "").trim();

        try {
          const batchAnalysis = JSON.parse(cleanContent);

          // Validate the response structure
          if (!batchAnalysis || typeof batchAnalysis !== "object") {
            throw new Error("Invalid response format");
          }

          // Ensure all required fields exist and are arrays
          const requiredFields = [
            "positive_points",
            "negative_points",
            "common_themes",
            "areas_for_improvement",
          ];
          for (const field of requiredFields) {
            if (!Array.isArray(batchAnalysis[field])) {
              batchAnalysis[field] = [];
            }
          }

          // Combine results
          allResults.overall_sentiment = batchAnalysis.overall_sentiment || "";
          allResults.positive_points = [
            ...new Set([
              ...allResults.positive_points,
              ...batchAnalysis.positive_points,
            ]),
          ];
          allResults.negative_points = [
            ...new Set([
              ...allResults.negative_points,
              ...batchAnalysis.negative_points,
            ]),
          ];
          allResults.common_themes = [
            ...new Set([
              ...allResults.common_themes,
              ...batchAnalysis.common_themes,
            ]),
          ];
          allResults.areas_for_improvement = [
            ...new Set([
              ...allResults.areas_for_improvement,
              ...batchAnalysis.areas_for_improvement,
            ]),
          ];
        } catch (parseError) {
          console.error(
            `[${this.hotel.name}] Error parsing OpenAI response:`,
            parseError
          );
          console.error("Raw response:", content);
          continue; // Skip this batch but continue with others
        }

        // Add delay between batches to respect rate limits
        if (i < batches.length - 1) {
          await new Promise((resolve) => setTimeout(resolve, 60000)); // 1 minute delay
        }
      }

      // Format the results for better readability in Looker Studio
      const formattedResults = {
        overall_sentiment: allResults.overall_sentiment,
        positive_points: allResults.positive_points
          .map((point) => `• ${point}`)
          .join("\n"),
        negative_points: allResults.negative_points
          .map((point) => `• ${point}`)
          .join("\n"),
        common_themes: allResults.common_themes
          .map((theme) => `• ${theme}`)
          .join("\n"),
        areas_for_improvement: allResults.areas_for_improvement
          .map((area) => `• ${area}`)
          .join("\n"),
      };

      // Store the single consolidated analysis result
      await this.bigquery.query({
        query: `
          INSERT INTO \`${this.config.projectId}.${this.config.datasetId}.${this.hotel.tableId}_analysis\`
          (analysis_date, total_reviews, analyzed_reviews, days_analyzed, overall_sentiment, positive_points, negative_points, common_themes, areas_for_improvement)
          VALUES (@analysis_date, @total_reviews, @analyzed_reviews, @days_analyzed, @overall_sentiment, @positive_points, @negative_points, @common_themes, @areas_for_improvement)
        `,
        params: {
          analysis_date: new Date().toISOString(),
          total_reviews: totalCount,
          analyzed_reviews: reviews.length, // Number of reviews in this batch
          days_analyzed: days,
          overall_sentiment: formattedResults.overall_sentiment,
          positive_points: formattedResults.positive_points,
          negative_points: formattedResults.negative_points,
          common_themes: formattedResults.common_themes,
          areas_for_improvement: formattedResults.areas_for_improvement,
        },
      });

      // Store the IDs of the analyzed reviews
      const analyzedIdsRows = reviews.map((review) => ({
        review_id: review.review_id,
        analysis_timestamp: new Date().toISOString(),
      }));

      if (analyzedIdsRows.length > 0) {
        await this.bigquery
          .dataset(this.config.datasetId)
          .table("AnalyzedReviewIDs")
          .insert(analyzedIdsRows);
        console.log(
          `[${this.hotel.name}] Stored ${analyzedIdsRows.length} analyzed review IDs`
        );
      }

      console.log(`[${this.hotel.name}] Analysis completed and stored`);
    } catch (error) {
      console.error(`[${this.hotel.name}] Error analyzing reviews:`, error);
      throw error;
    }
  }

  async ensureAnalysisTableExists() {
    try {
      // Ensure Hotel-Specific Analysis Table
      const analysisTableId = `${this.hotel.tableId}_analysis`;
      const table = this.bigquery
        .dataset(this.config.datasetId)
        .table(analysisTableId);
      const [exists] = await table.exists();

      const requiredSchema = [
        { name: "analysis_date", type: "TIMESTAMP", mode: "REQUIRED" },
        { name: "total_reviews", type: "INTEGER", mode: "REQUIRED" },
        { name: "analyzed_reviews", type: "INTEGER", mode: "REQUIRED" },
        { name: "days_analyzed", type: "INTEGER", mode: "REQUIRED" },
        { name: "overall_sentiment", type: "STRING", mode: "REQUIRED" },
        { name: "positive_points", type: "STRING", mode: "REQUIRED" },
        { name: "negative_points", type: "STRING", mode: "REQUIRED" },
        { name: "common_themes", type: "STRING", mode: "REQUIRED" },
        { name: "areas_for_improvement", type: "STRING", mode: "REQUIRED" },
      ];

      if (exists) {
        console.log(
          `[${this.hotel.name}] Analysis table already exists. Schema verification skipped (review_id removed).`
        );
        // Optionally add more detailed schema verification if needed in the future
      } else {
        console.log(`[${this.hotel.name}] Creating analysis table...`);
        await this.bigquery
          .dataset(this.config.datasetId)
          .createTable(analysisTableId, { schema: requiredSchema });
        console.log(
          `[${this.hotel.name}] Analysis table created successfully.`
        );
      }

      // Ensure Central AnalyzedReviewIDs Table
      const analyzedIdsTableId = "AnalyzedReviewIDs";
      const analyzedIdsTable = this.bigquery
        .dataset(this.config.datasetId)
        .table(analyzedIdsTableId);
      const [analyzedIdsTableExists] = await analyzedIdsTable.exists();

      if (!analyzedIdsTableExists) {
        console.warn(
          `Central ${analyzedIdsTableId} table missing! Attempting to create based on setup script schema...`
        );
        const analyzedReviewIdsSchema = [
          { name: "review_id", type: "STRING", mode: "REQUIRED" },
          { name: "analysis_timestamp", type: "TIMESTAMP", mode: "REQUIRED" },
        ];
        await this.bigquery
          .dataset(this.config.datasetId)
          .createTable(analyzedIdsTableId, { schema: analyzedReviewIdsSchema })
          .catch((err) => {
            console.error(`Failed to create ${analyzedIdsTableId} table:`, err);
            // Decide if we should throw error or try to continue
          });
        console.log(`Central ${analyzedIdsTableId} table potentially created.`);
      }
    } catch (error) {
      console.error(
        `[${this.hotel.name}] Error ensuring analysis table exists:`,
        error
      );
      throw error;
    }
  }

  async analyzeRecentReviewsByCount(reviewCount = 30) {
    try {
      // Ensure analysis table exists with correct schema
      await this.ensureAnalysisTableExists();

      // Get the latest total review count
      const totalCount = await this.getLatestTotalCount();
      if (!totalCount) {
        throw new Error("Could not get total review count");
      }

      console.log(
        `[${this.hotel.name}] Total reviews in database: ${totalCount}`
      );

      // First, check if there are any reviews in the table
      const [countResult] = await this.bigquery.query({
        query: `
          SELECT COUNT(*) as count
          FROM \`${this.config.projectId}.${this.config.datasetId}.${this.hotel.tableId}\`
        `,
      });

      const totalReviewsInTable = countResult[0].count;
      console.log(
        `[${this.hotel.name}] Reviews in table: ${totalReviewsInTable}`
      );

      if (totalReviewsInTable === 0) {
        console.log(
          `[${this.hotel.name}] No reviews found in the table. Please run the daily update first.`
        );
        return;
      }

      // Check how many reviews have already been analyzed
      const [analyzedCountResult] = await this.bigquery.query({
        query: `
          SELECT COUNT(DISTINCT review_id) as count
          FROM \`${this.config.projectId}.${this.config.datasetId}.AnalyzedReviewIDs\`
        `,
      });

      const analyzedReviewsCount = analyzedCountResult[0].count;
      console.log(
        `[${this.hotel.name}] Already analyzed reviews: ${analyzedReviewsCount}`
      );

      // Get recent reviews that haven't been analyzed yet
      const [reviews] = await this.bigquery.query({
        query: `
          SELECT *
          FROM \`${this.config.projectId}.${this.config.datasetId}.${this.hotel.tableId}\`
          WHERE review_id NOT IN (
            SELECT review_id 
            FROM \`${this.config.projectId}.${this.config.datasetId}.AnalyzedReviewIDs\`
          )
          ORDER BY entry_date DESC
          LIMIT ${reviewCount}
        `,
      });

      if (reviews.length === 0) {
        console.log(
          `[${this.hotel.name}] No new reviews to analyze. All reviews have already been analyzed.`
        );
        return;
      }

      console.log(
        `[${this.hotel.name}] Analyzing ${reviews.length} recent reviews`
      );

      // Process reviews in batches of 10
      const batchSize = 10;
      const batches = [];
      for (let i = 0; i < reviews.length; i += batchSize) {
        batches.push(reviews.slice(i, i + batchSize));
      }

      let allResults = {
        overall_sentiment: "",
        positive_points: [],
        negative_points: [],
        common_themes: [],
        areas_for_improvement: [],
      };

      // Process each batch
      for (let i = 0; i < batches.length; i++) {
        const batch = batches[i];
        console.log(
          `[${this.hotel.name}] Processing batch ${i + 1}/${batches.length} (${
            batch.length
          } reviews)`
        );

        const reviewsText = batch
          .map(
            (r) =>
              `Bewertung: ${r.rating_general}/6\nTitel: ${r.title}\nKommentar: ${r.general_text}\n`
          )
          .join("\n");

        const response = await this.openai.chat.completions.create({
          model: "gpt-4",
          messages: [
            {
              role: "system",
              content: `Du bist ein Experte für Hotelbewertungen. Analysiere die folgenden Bewertungen und erstelle eine Zusammenfassung im folgenden JSON-Format (ohne Markdown-Formatierung):
{
  "overall_sentiment": "Gesamteindruck der Bewertungen",
  "positive_points": ["Positivpunkt 1", "Positivpunkt 2", ...],
  "negative_points": ["Negativpunkt 1", "Negativpunkt 2", ...],
  "common_themes": ["Hauptthema 1", "Hauptthema 2", ...],
  "areas_for_improvement": ["Verbesserungsbereich 1", "Verbesserungsbereich 2", ...]
}`,
            },
            {
              role: "user",
              content: `Analysiere diese Hotelbewertungen:\n\n${reviewsText}`,
            },
          ],
          temperature: 0.7,
        });

        const content = response.choices[0].message.content;

        // Clean the response - remove markdown formatting if present
        const cleanContent = content.replace(/```json\n?|\n?```/g, "").trim();

        try {
          const batchAnalysis = JSON.parse(cleanContent);

          // Validate the response structure
          if (!batchAnalysis || typeof batchAnalysis !== "object") {
            throw new Error("Invalid response format");
          }

          // Ensure all required fields exist and are arrays
          const requiredFields = [
            "positive_points",
            "negative_points",
            "common_themes",
            "areas_for_improvement",
          ];
          for (const field of requiredFields) {
            if (!Array.isArray(batchAnalysis[field])) {
              batchAnalysis[field] = [];
            }
          }

          // Combine results
          allResults.overall_sentiment = batchAnalysis.overall_sentiment || "";
          allResults.positive_points = [
            ...new Set([
              ...allResults.positive_points,
              ...batchAnalysis.positive_points,
            ]),
          ];
          allResults.negative_points = [
            ...new Set([
              ...allResults.negative_points,
              ...batchAnalysis.negative_points,
            ]),
          ];
          allResults.common_themes = [
            ...new Set([
              ...allResults.common_themes,
              ...batchAnalysis.common_themes,
            ]),
          ];
          allResults.areas_for_improvement = [
            ...new Set([
              ...allResults.areas_for_improvement,
              ...batchAnalysis.areas_for_improvement,
            ]),
          ];
        } catch (parseError) {
          console.error(
            `[${this.hotel.name}] Error parsing OpenAI response:`,
            parseError
          );
          console.error("Raw response:", content);
          continue; // Skip this batch but continue with others
        }

        // Add delay between batches to respect rate limits
        if (i < batches.length - 1) {
          await new Promise((resolve) => setTimeout(resolve, 60000)); // 1 minute delay
        }
      }

      // Format the results for better readability in Looker Studio
      const formattedResults = {
        overall_sentiment: allResults.overall_sentiment,
        positive_points: allResults.positive_points
          .map((point) => `• ${point}`)
          .join("\n"),
        negative_points: allResults.negative_points
          .map((point) => `• ${point}`)
          .join("\n"),
        common_themes: allResults.common_themes
          .map((theme) => `• ${theme}`)
          .join("\n"),
        areas_for_improvement: allResults.areas_for_improvement
          .map((area) => `• ${area}`)
          .join("\n"),
      };

      // Store the single consolidated analysis result
      await this.bigquery.query({
        query: `
          INSERT INTO \`${this.config.projectId}.${this.config.datasetId}.${this.hotel.tableId}_analysis\`
          (analysis_date, total_reviews, analyzed_reviews, days_analyzed, overall_sentiment, positive_points, negative_points, common_themes, areas_for_improvement)
          VALUES (@analysis_date, @total_reviews, @analyzed_reviews, @days_analyzed, @overall_sentiment, @positive_points, @negative_points, @common_themes, @areas_for_improvement)
        `,
        params: {
          analysis_date: new Date().toISOString(),
          total_reviews: totalCount,
          analyzed_reviews: reviews.length, // Number of reviews in this batch
          days_analyzed: 0, // Not applicable when analyzing by count
          overall_sentiment: formattedResults.overall_sentiment,
          positive_points: formattedResults.positive_points,
          negative_points: formattedResults.negative_points,
          common_themes: formattedResults.common_themes,
          areas_for_improvement: formattedResults.areas_for_improvement,
        },
      });

      // Store the IDs of the analyzed reviews
      const analyzedIdsRows = reviews.map((review) => ({
        review_id: review.review_id,
        analysis_timestamp: new Date().toISOString(),
      }));

      if (analyzedIdsRows.length > 0) {
        await this.bigquery
          .dataset(this.config.datasetId)
          .table("AnalyzedReviewIDs")
          .insert(analyzedIdsRows);
        console.log(
          `[${this.hotel.name}] Stored ${analyzedIdsRows.length} analyzed review IDs`
        );
      }

      console.log(`[${this.hotel.name}] Analysis completed and stored`);
    } catch (error) {
      console.error(`[${this.hotel.name}] Error analyzing reviews:`, error);
      throw error;
    }
  }

  async storeAnalysis(analysis, days) {
    try {
      const query = `
        INSERT INTO \`${this.config.projectId}.${this.config.datasetId}.${this.hotel.tableId}_analysis\`
        (analysis_date, days_analyzed, overall_sentiment, positive_points, negative_points, common_themes, areas_for_improvement)
        VALUES
        (CURRENT_TIMESTAMP(), @days, @sentiment, @positive, @negative, @themes, @improvements)
      `;

      const options = {
        query,
        location: "US",
        params: {
          days,
          sentiment: analysis.overall_sentiment,
          positive: analysis.positive_points,
          negative: analysis.negative_points,
          themes: analysis.common_themes,
          improvements: analysis.areas_for_improvement,
        },
      };

      await this.bigquery.query(options);
      console.log(
        `[${this.hotel.name}] Stored analysis for the last ${days} days`
      );
    } catch (error) {
      console.error(`[${this.hotel.name}] Error storing analysis:`, error);
      throw error;
    }
  }
}
