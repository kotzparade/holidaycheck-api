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

  async getTotalReviews() {
    try {
      const query = `
        SELECT total_reviews
        FROM \`${this.config.projectId}.${this.config.datasetId}.ReviewTotals\`
        WHERE hotel_id = @hotelId
        ORDER BY last_updated DESC
        LIMIT 1
      `;

      const [rows] = await this.bigquery.query({
        query,
        location: "US",
        params: { hotelId: this.hotel.id },
      });

      return rows[0]?.total_reviews || 0;
    } catch (error) {
      console.error(
        `[${this.hotel.name}] Error fetching total reviews:`,
        error
      );
      return 0;
    }
  }

  async analyzeRecentReviews(
    days = parseInt(process.env.DEFAULT_ANALYSIS_DAYS) || 30
  ) {
    try {
      // Get total review count
      const totalReviews = await this.getTotalReviews();
      console.log(`[${this.hotel.name}] Total reviews: ${totalReviews}`);

      // Fetch reviews from the last X days
      const query = `
        SELECT 
          review_id,
          title,
          general_text,
          rating_general,
          entry_date,
          travel_date
        FROM \`${this.config.projectId}.${this.config.datasetId}.${this.hotel.tableId}\`
        WHERE entry_date >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @days DAY)
        ORDER BY entry_date DESC
        LIMIT 50  -- Limit the number of reviews to analyze
      `;

      const [reviews] = await this.bigquery.query({
        query,
        location: "US",
        params: { days },
      });

      if (!reviews.length) {
        console.log(
          `[${this.hotel.name}] No reviews found in the last ${days} days`
        );
        return;
      }

      // Process reviews in batches of 10
      const batchSize = 10;
      const batches = [];
      for (let i = 0; i < reviews.length; i += batchSize) {
        batches.push(reviews.slice(i, i + batchSize));
      }

      let combinedAnalysis = {
        overall_sentiment: "",
        positive_points: [],
        negative_points: [],
        common_themes: [],
        areas_for_improvement: [],
        total_reviews: totalReviews,
        analyzed_reviews: reviews.length,
        analysis_period_days: days,
      };

      // Process each batch
      for (let i = 0; i < batches.length; i++) {
        const batch = batches[i];
        console.log(
          `[${this.hotel.name}] Processing batch ${i + 1}/${batches.length} (${
            batch.length
          } reviews)`
        );

        // Prepare review text for analysis
        const reviewTexts = batch
          .map(
            (review) =>
              `Titel: ${review.title}\nBewertung: ${review.general_text}`
          )
          .join("\n\n");

        // Analyze with OpenAI
        const analysis = await this.openai.chat.completions.create({
          model: "gpt-4",
          messages: [
            {
              role: "system",
              content: `Du bist ein Hotel-Bewertungsanalyst. Analysiere die folgenden Bewertungen für ${this.hotel.name} und gib Einblicke zu:
              1. Gesamteindruck (ein Satz)
              2. Wichtige positive Aspekte (Liste von 2-3 Punkten)
              3. Wichtige negative Aspekte (Liste von 2-3 Punkten)
              4. Häufige Themen (Liste von 2-3 Themen)
              5. Verbesserungspotenzial (Liste von 2-3 Bereichen)
              
              Formatiere die Antwort als JSON mit folgender Struktur:
              {
                "overall_sentiment": "string",
                "positive_points": ["string"],
                "negative_points": ["string"],
                "common_themes": ["string"],
                "areas_for_improvement": ["string"]
              }
              
              WICHTIG: 
              - Alle Texte müssen auf Deutsch sein
              - Formuliere die Punkte kurz und prägnant
              - Vermeide Wiederholungen
              - Verwende keine Aufzählungszeichen oder Nummerierung in den Texten
              - Formuliere negative Punkte konstruktiv`,
            },
            {
              role: "user",
              content: reviewTexts,
            },
          ],
          temperature: parseFloat(process.env.TEMPERATURE) || 0.7,
          max_tokens: parseInt(process.env.MAX_TOKENS) || 1000,
        });

        let batchAnalysis;
        try {
          // Clean the response by removing markdown code block formatting
          const cleanResponse = analysis.choices[0].message.content
            .replace(/```json\n?/, "") // Remove opening ```json
            .replace(/```$/, "") // Remove closing ```
            .trim(); // Remove any extra whitespace

          batchAnalysis = JSON.parse(cleanResponse);

          // Validate the response structure
          if (!batchAnalysis || typeof batchAnalysis !== "object") {
            throw new Error("Invalid response format");
          }

          // Ensure all required fields exist and are arrays
          batchAnalysis = {
            overall_sentiment: batchAnalysis.overall_sentiment || "",
            positive_points: Array.isArray(batchAnalysis.positive_points)
              ? batchAnalysis.positive_points
              : [],
            negative_points: Array.isArray(batchAnalysis.negative_points)
              ? batchAnalysis.negative_points
              : [],
            common_themes: Array.isArray(batchAnalysis.common_themes)
              ? batchAnalysis.common_themes
              : [],
            areas_for_improvement: Array.isArray(
              batchAnalysis.areas_for_improvement
            )
              ? batchAnalysis.areas_for_improvement
              : [],
          };

          // Combine analyses
          combinedAnalysis.overall_sentiment = batchAnalysis.overall_sentiment;
          combinedAnalysis.positive_points = [
            ...new Set([
              ...combinedAnalysis.positive_points,
              ...batchAnalysis.positive_points,
            ]),
          ];
          combinedAnalysis.negative_points = [
            ...new Set([
              ...combinedAnalysis.negative_points,
              ...batchAnalysis.negative_points,
            ]),
          ];
          combinedAnalysis.common_themes = [
            ...new Set([
              ...combinedAnalysis.common_themes,
              ...batchAnalysis.common_themes,
            ]),
          ];
          combinedAnalysis.areas_for_improvement = [
            ...new Set([
              ...combinedAnalysis.areas_for_improvement,
              ...batchAnalysis.areas_for_improvement,
            ]),
          ];
        } catch (parseError) {
          console.error(
            `[${this.hotel.name}] Error parsing OpenAI response:`,
            parseError
          );
          console.log("Raw response:", analysis.choices[0].message.content);
          continue; // Skip this batch and continue with the next one
        }

        // Add delay between batches to respect rate limits
        if (i < batches.length - 1) {
          await new Promise((resolve) => setTimeout(resolve, 60000)); // 1 minute delay
        }
      }

      // Format the analysis for better readability in Looker Studio
      const formattedAnalysis = {
        overall_sentiment: combinedAnalysis.overall_sentiment,
        positive_points: combinedAnalysis.positive_points
          .map((point) => `• ${point}\n`)
          .join("\n"),
        negative_points: combinedAnalysis.negative_points
          .map((point) => `• ${point}\n`)
          .join("\n"),
        common_themes: combinedAnalysis.common_themes
          .map((theme) => `• ${theme}\n`)
          .join("\n"),
        areas_for_improvement: combinedAnalysis.areas_for_improvement
          .map((area) => `• ${area}\n`)
          .join("\n"),
        total_reviews: combinedAnalysis.total_reviews,
        analyzed_reviews: combinedAnalysis.analyzed_reviews,
        analysis_period_days: combinedAnalysis.analysis_period_days,
      };

      // Store combined analysis results
      await this.storeAnalysis(formattedAnalysis, days);

      return formattedAnalysis;
    } catch (error) {
      console.error(`[${this.hotel.name}] Error analyzing reviews:`, error);
      throw error;
    }
  }

  async storeAnalysis(analysis, days) {
    try {
      const query = `
        INSERT INTO \`${this.config.projectId}.${this.config.datasetId}.${this.hotel.tableId}_analysis\`
        (analysis_date, days_analyzed, overall_sentiment, positive_points, negative_points, common_themes, areas_for_improvement, total_reviews, analyzed_reviews)
        VALUES
        (CURRENT_TIMESTAMP(), @days, @sentiment, @positive, @negative, @themes, @improvements, @total_reviews, @analyzed_reviews)
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
          total_reviews: analysis.total_reviews,
          analyzed_reviews: analysis.analyzed_reviews,
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
