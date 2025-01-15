use std::collections::HashMap;
use super::text_processor::{TsVector, Token, ProcessedQuery, TokenType, QueryOperator};

/// Normalization options for document ranking
#[derive(Debug, Clone, Copy)]
pub enum RankNormalization {
    /// 0 (Default) - Ignores document length
    None = 0,
    /// 1 - Divides by 1 + log(length)
    LogLength = 1,
    /// 2 - Divides by length
    Length = 2,
    /// 4 - Divides by mean harmonic distance between extents
    MeanHarmonic = 4,
    /// 8 - Divides by unique word count
    UniqueWordCount = 8,
    /// 16 - Divides by 1 + log(unique word count)
    LogUniqueWordCount = 16,
    /// 32 - Divides by unique word count + 1
    UniqueWordCountPlusOne = 32,
}

/// Weight categories for lexemes (D, C, B, A)
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum LexemeWeight {
    D = 0,
    C = 1,
    B = 2,
    A = 3,
}

impl LexemeWeight {
    pub fn from_position(position: usize) -> Self {
        // Weight assignment based on position in text
        match position {
            0..=10 => LexemeWeight::A,    // First 10 words get highest weight
            11..=25 => LexemeWeight::B,   // Next 15 words get high weight
            26..=50 => LexemeWeight::C,   // Next 25 words get medium weight
            _ => LexemeWeight::D,         // Rest get lowest weight
        }
    }
}

/// BM25 ranking parameters
#[derive(Debug, Clone, Copy)]
pub struct BM25Params {
    /// k1 parameter controls term frequency saturation (typically between 1.2 and 2.0)
    pub k1: f64,
    /// b parameter controls document length normalization (typically 0.75)
    pub b: f64,
}

impl Default for BM25Params {
    fn default() -> Self {
        BM25Params {
            k1: 1.5,
            b: 0.75,
        }
    }
}

/// TF-IDF normalization options
#[derive(Debug, Clone, Copy)]
pub enum TfIdfNormalization {
    /// No normalization
    None,
    /// L1 normalization (Manhattan)
    L1,
    /// L2 normalization (Euclidean)
    L2,
    /// Max normalization (divide by maximum tf)
    Max,
    /// Log normalization (1 + log(tf))
    Log,
    /// Double normalization with K (default K = 0.5)
    DoubleNormK(f64),
}

/// TF-IDF configuration parameters
#[derive(Debug, Clone)]
pub struct TfIdfParams {
    /// How to normalize term frequencies
    pub tf_normalization: TfIdfNormalization,
    /// How to normalize document vectors
    pub doc_normalization: TfIdfNormalization,
    /// Whether to use smoothed IDF (log((N+1)/(df+1)) + 1)
    pub use_smoothed_idf: bool,
    /// Whether to apply document length penalty
    pub use_length_penalty: bool,
}

impl Default for TfIdfParams {
    fn default() -> Self {
        TfIdfParams {
            tf_normalization: TfIdfNormalization::Log,
            doc_normalization: TfIdfNormalization::L2,
            use_smoothed_idf: true,
            use_length_penalty: true,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RankingConfig {
    /// Weights for document categories (D, C, B, A)
    pub weights: [f32; 4],
    /// Normalization options (can be combined using bitwise OR)
    pub normalization: u32,
    /// Whether to use IDF in scoring
    pub use_idf: bool,
    /// Whether to consider lexeme weights
    pub use_lexeme_weights: bool,
    /// BM25 parameters (None means don't use BM25)
    pub bm25_params: Option<BM25Params>,
}

impl Default for RankingConfig {
    fn default() -> Self {
        RankingConfig {
            weights: [0.1, 0.2, 0.4, 1.0],
            normalization: 0,
            use_idf: true,
            use_lexeme_weights: true,
            bm25_params: None,
        }
    }
}

pub trait RankingSystem {
    /// Calculate rank using standard ranking algorithm
    fn rank(&self, doc: &TsVector, query: &ProcessedQuery, config: &RankingConfig) -> f64;
    
    /// Calculate rank using cover density ranking algorithm
    fn rank_cd(&self, doc: &TsVector, query: &ProcessedQuery, config: &RankingConfig) -> f64;
}

/// BM25 ranking implementation
pub struct BM25Ranking {
    collection_stats: Option<CollectionStats>,
    avg_doc_length: f64,
}

impl BM25Ranking {
    pub fn new() -> Self {
        BM25Ranking {
            collection_stats: Some(CollectionStats::default()),
            avg_doc_length: 0.0,
        }
    }

    pub fn with_collection_stats(
        total_docs: usize,
        term_doc_frequencies: HashMap<String, usize>,
        avg_doc_length: f64,
    ) -> Self {
        BM25Ranking {
            collection_stats: Some(CollectionStats {
                total_docs,
                term_doc_frequencies,
            }),
            avg_doc_length,
        }
    }

    fn calculate_idf(&self, term: &str) -> f64 {
        if let Some(stats) = &self.collection_stats {
            let doc_freq = stats.term_doc_frequencies.get(term).copied().unwrap_or(1);
            // Ensure IDF is always positive by using ln(1 + (N - n + 0.5)/(n + 0.5))
            (1.0 + (stats.total_docs as f64 - doc_freq as f64 + 0.5) / (doc_freq as f64 + 0.5)).ln()
        } else {
            1.0 // Default IDF when no collection stats available
        }
    }
    

    fn calculate_bm25_score(
        &self,
        doc: &TsVector,
        query: &ProcessedQuery,
        params: &BM25Params,
    ) -> f64 {
        let doc_length = doc.tokens.len() as f64;
        let mut score = 0.0;

        for query_token in &query.tokens {
            let term_occurrences = self.calculate_term_frequency(doc, &query_token.text);
            if !term_occurrences.is_empty() {
                let term_freq = term_occurrences.len() as f64;
                let idf = self.calculate_idf(&query_token.text);
                
                // BM25 term frequency normalization
                let numerator = term_freq * (params.k1 + 1.0);
                let length_norm = 1.0 - params.b + params.b * (doc_length / self.avg_doc_length.max(1.0));
                let denominator = term_freq + params.k1 * length_norm;
                
                // Base BM25 score for this term
                let term_score = idf * numerator / denominator;
                
                // Apply position weights with stronger early position bias
                for (pos, weight) in term_occurrences {
                    let position_weight = match pos {
                        1 => 256.0,  // Highest weight for first position
                        2 => 128.0,
                        3 => 64.0,
                        4 => 32.0,
                        5 => 16.0,
                        6 => 8.0,
                        _ => 1.0,
                    };
                    score += term_score * position_weight * weight as f64;
                }
            }
        }

        score
    }
}

impl RankingSystem for BM25Ranking {
    fn rank(&self, doc: &TsVector, query: &ProcessedQuery, config: &RankingConfig) -> f64 {
        if let Some(bm25_params) = config.bm25_params {
            let raw_score = self.calculate_bm25_score(doc, query, &bm25_params);
            let normalized_score = self.apply_normalization(raw_score, doc, config);
            if config.use_lexeme_weights {
                normalized_score
            } else {
                // If lexeme weights are disabled, normalize out their effect
                normalized_score / 256.0 // Divide by max position boost
            }
        } else {
            // Fallback to simpler TF-IDF if BM25 params not provided
            let mut score = 0.0;
            for query_token in &query.tokens {
                let term_occurrences = self.calculate_term_frequency(doc, &query_token.text);
                if !term_occurrences.is_empty() {
                    let tf = term_occurrences.len() as f64;
                    let idf = if config.use_idf {
                        self.calculate_idf(&query_token.text)
                    } else {
                        1.0
                    };

                    for (pos, weight) in term_occurrences {
                        let lexeme_weight = if config.use_lexeme_weights {
                            self.calculate_lexeme_weight(pos, config)
                        } else {
                            1.0
                        };
                        score += tf * idf * weight as f64 * lexeme_weight;
                    }
                }
            }
            self.apply_normalization(score, doc, config)
        }
    }

    fn rank_cd(&self, doc: &TsVector, query: &ProcessedQuery, config: &RankingConfig) -> f64 {
        let base_score = self.rank(doc, query, config);
        let cover_density = self.calculate_cover_density(doc, query);
        
        // Combine base score with cover density
        if base_score > 0.0 && cover_density > 0.0 {
            base_score * (1.0 + cover_density)
        } else {
            base_score
        }
    }
}

#[derive(Debug, Default)]
struct CollectionStats {
    total_docs: usize,
    term_doc_frequencies: HashMap<String, usize>,
}

impl BM25Ranking {
    fn calculate_term_frequency(&self, doc: &TsVector, term: &str) -> Vec<(usize, f32)> {
        doc.tokens
            .iter()
            .filter(|t| t.text == term)
            .map(|t| (t.position, t.weight))
            .collect()
    }

    fn calculate_unique_terms(&self, doc: &TsVector) -> usize {
        let mut unique_terms = HashMap::new();
        for token in &doc.tokens {
            unique_terms.insert(&token.text, true);
        }
        unique_terms.len()
    }

    fn calculate_lexeme_weight(&self, position: usize, config: &RankingConfig) -> f64 {
        if !config.use_lexeme_weights {
            return 1.0;
        }

        // Get category weight from config (position is 1-based from TsVector)
        let weight_category = LexemeWeight::from_position(position - 1) as usize;
        let category_weight = config.weights[weight_category] as f64;

        // Apply exponential position boost with stronger early position bias
        let position_boost = match position {
            1 => 512.0,  // Doubled from 256.0
            2 => 256.0,  // Doubled from 128.0
            3 => 128.0,  // Doubled from 64.0
            4 => 64.0,   // Doubled from 32.0
            5 => 32.0,   // Doubled from 16.0
            6 => 16.0,   // Doubled from 8.0
            _ => 1.0,
        };

        // Multiply category weight by position boost and add position-based bonus
        category_weight * position_boost * (2.0 + (1.0 / position as f64))
    }

    fn calculate_cover_density(&self, doc: &TsVector, query: &ProcessedQuery) -> f64 {
        if query.tokens.len() < 2 {
            return 1.0;
        }

        let mut positions: Vec<_> = doc.tokens.iter()
            .filter(|t| query.tokens.iter().any(|qt| qt.text == t.text))
            .map(|t| t.position)
            .collect();
        positions.sort_unstable();

        if positions.len() < 2 {
            return 0.0;
        }

        // Calculate minimum span (distance between first and last matching terms)
        let min_span = positions.last().unwrap() - positions.first().unwrap() + 1;
        
        // Stronger proximity penalty with exponential decay
        let proximity_score = 1.0 / (min_span as f64).powf(3.0);
        
        // Enhanced density scoring with higher weight for complete matches
        let density_score = (positions.len() as f64 / query.tokens.len() as f64).powf(2.0);
        
        // Calculate average distance between consecutive terms
        let avg_distance = if positions.len() > 1 {
            let mut total_distance = 0;
            for i in 1..positions.len() {
                total_distance += positions[i] - positions[i-1];
            }
            total_distance as f64 / (positions.len() - 1) as f64
        } else {
            min_span as f64
        };

        // Combine scores with higher weights on proximity and density
        let combined_score = proximity_score * density_score * (1.0 + 1.0 / avg_distance.powf(2.0));
        combined_score * 128.0 // Increased base multiplier
    }

    fn apply_normalization(&self, score: f64, doc: &TsVector, config: &RankingConfig) -> f64 {
        let mut normalized = score;
        let doc_length = doc.tokens.len();
        let unique_terms = self.calculate_unique_terms(doc);

        if config.normalization & (RankNormalization::LogLength as u32) != 0 {
            normalized /= 1.0 + (doc_length as f64).ln();
        }
        if config.normalization & (RankNormalization::Length as u32) != 0 {
            normalized /= doc_length as f64;
        }
        if config.normalization & (RankNormalization::UniqueWordCount as u32) != 0 {
            normalized /= unique_terms as f64;
        }
        if config.normalization & (RankNormalization::LogUniqueWordCount as u32) != 0 {
            normalized /= 1.0 + (unique_terms as f64).ln();
        }
        if config.normalization & (RankNormalization::UniqueWordCountPlusOne as u32) != 0 {
            normalized /= (unique_terms + 1) as f64;
        }
        if config.normalization & (RankNormalization::MeanHarmonic as u32) != 0 {
            // Apply harmonic mean normalization for term positions
            let positions: Vec<_> = doc.tokens.iter().map(|t| t.position).collect();
            if !positions.is_empty() {
                let sum_reciprocals: f64 = positions.iter().map(|&p| 1.0 / p as f64).sum();
                normalized /= positions.len() as f64 / sum_reciprocals;
            }
        }

        normalized
    }
}

impl Default for BM25Ranking {
    fn default() -> Self {
        Self::new()
    }
}

/// TF-IDF ranking implementation
pub struct TfIdfRanking {
    collection_stats: Option<CollectionStats>,
    avg_doc_length: f64,
}

impl TfIdfRanking {
    pub fn new() -> Self {
        TfIdfRanking {
            collection_stats: Some(CollectionStats::default()),
            avg_doc_length: 0.0,
        }
    }

    pub fn with_collection_stats(
        total_docs: usize,
        term_doc_frequencies: HashMap<String, usize>,
        avg_doc_length: f64,
    ) -> Self {
        TfIdfRanking {
            collection_stats: Some(CollectionStats {
                total_docs,
                term_doc_frequencies,
            }),
            avg_doc_length,
        }
    }

    fn calculate_normalized_tf(&self, raw_tf: f64, normalization: TfIdfNormalization) -> f64 {
        match normalization {
            TfIdfNormalization::None => raw_tf,
            TfIdfNormalization::L1 => raw_tf,
            TfIdfNormalization::L2 => raw_tf.sqrt(),
            TfIdfNormalization::Max => raw_tf, // Will be normalized later by max tf
            TfIdfNormalization::Log => if raw_tf > 0.0 { 1.0 + raw_tf.ln() } else { 0.0 },
            TfIdfNormalization::DoubleNormK(k) => k + (1.0 - k) * raw_tf,
        }
    }

    fn calculate_smoothed_idf(&self, term: &str) -> f64 {
        if let Some(stats) = &self.collection_stats {
            let doc_freq = stats.term_doc_frequencies.get(term).copied().unwrap_or(1);
            ((stats.total_docs as f64 + 1.0) / (doc_freq as f64 + 1.0)).ln() + 1.0
        } else {
            1.0
        }
    }

    fn calculate_idf(&self, term: &str) -> f64 {
        if let Some(stats) = &self.collection_stats {
            let doc_freq = stats.term_doc_frequencies.get(term).copied().unwrap_or(1);
            ((stats.total_docs as f64) / (doc_freq as f64)).ln()
        } else {
            1.0
        }
    }

    fn calculate_lexeme_weight(&self, position: usize, config: &RankingConfig) -> f64 {
        if !config.use_lexeme_weights {
            return 1.0;
        }

        let weight_category = LexemeWeight::from_position(position - 1) as usize;
        let category_weight = config.weights[weight_category] as f64;

        let position_boost = match position {
            1 => 512.0,
            2 => 256.0,
            3 => 128.0,
            4 => 64.0,
            5 => 32.0,
            6 => 16.0,
            _ => 1.0,
        };

        category_weight * position_boost * (2.0 + (1.0 / position as f64))
    }

    fn calculate_cover_density(&self, doc: &TsVector, query: &ProcessedQuery) -> f64 {
        if query.tokens.len() < 2 {
            return 1.0;
        }

        let mut positions: Vec<_> = doc.tokens.iter()
            .filter(|t| query.tokens.iter().any(|qt| qt.text == t.text))
            .map(|t| t.position)
            .collect();
        positions.sort_unstable();

        if positions.len() < 2 {
            return 0.0;
        }

        let min_span = positions.last().unwrap() - positions.first().unwrap() + 1;
        let proximity_score = 1.0 / (min_span as f64).powf(3.0);
        let density_score = (positions.len() as f64 / query.tokens.len() as f64).powf(2.0);
        
        let avg_distance = if positions.len() > 1 {
            let mut total_distance = 0;
            for i in 1..positions.len() {
                total_distance += positions[i] - positions[i-1];
            }
            total_distance as f64 / (positions.len() - 1) as f64
        } else {
            min_span as f64
        };

        let combined_score = proximity_score * density_score * (1.0 + 1.0 / avg_distance.powf(2.0));
        combined_score * 128.0
    }

    fn calculate_tfidf_score(
        &self,
        doc: &TsVector,
        query: &ProcessedQuery,
        params: &TfIdfParams,
    ) -> f64 {
        let mut term_scores = HashMap::new();
        let mut max_tf = 0.0_f64;
        
        // Calculate term frequencies and find max_tf
        for token in &doc.tokens {
            let count = term_scores.entry(token.text.clone()).or_insert(0.0);
            *count += 1.0;
            max_tf = max_tf.max(*count);
        }

        // Calculate normalized scores for each query term
        let mut score = 0.0;
        let mut normalization_factor = 0.0;

        for query_token in &query.tokens {
            let tf = *term_scores.get(&query_token.text).unwrap_or(&0.0);
            let mut normalized_tf = self.calculate_normalized_tf(tf, params.tf_normalization);
            
            // Apply max normalization if needed
            if matches!(params.tf_normalization, TfIdfNormalization::Max) {
                normalized_tf /= max_tf.max(1.0_f64);
            }

            let idf = if params.use_smoothed_idf {
                self.calculate_smoothed_idf(&query_token.text)
            } else {
                self.calculate_idf(&query_token.text)
            };

            let term_score = normalized_tf * idf;
            score += term_score;

            // Prepare normalization factor
            match params.doc_normalization {
                TfIdfNormalization::L1 => normalization_factor += term_score.abs(),
                TfIdfNormalization::L2 => normalization_factor += term_score * term_score,
                _ => {}
            }
        }

        // Apply document normalization
        match params.doc_normalization {
            TfIdfNormalization::L1 if normalization_factor > 0.0 => score /= normalization_factor,
            TfIdfNormalization::L2 if normalization_factor > 0.0 => score /= normalization_factor.sqrt(),
            _ => {}
        }

        // Apply length penalty if enabled - Modified to be more aggressive
        if params.use_length_penalty {
            let length_ratio = doc.tokens.len() as f64 / self.avg_doc_length.max(1.0_f64);
            // Square the length ratio to make the penalty more aggressive
            score /= length_ratio.powf(2.0);
        }

        score
    }
}

impl RankingSystem for TfIdfRanking {
    fn rank(&self, doc: &TsVector, query: &ProcessedQuery, config: &RankingConfig) -> f64 {
        // Create default TF-IDF params if not using BM25
        let params = TfIdfParams::default();
        let raw_score = self.calculate_tfidf_score(doc, query, &params);
        
        // Apply position weights if enabled
        if config.use_lexeme_weights {
            let mut weighted_score = 0.0;
            for token in &doc.tokens {
                if query.tokens.iter().any(|qt| qt.text == token.text) {
                    let position_weight = self.calculate_lexeme_weight(token.position, config);
                    weighted_score += raw_score * position_weight;
                }
            }
            weighted_score
        } else {
            raw_score
        }
    }

    fn rank_cd(&self, doc: &TsVector, query: &ProcessedQuery, config: &RankingConfig) -> f64 {
        let base_score = self.rank(doc, query, config);
        let cover_density = self.calculate_cover_density(doc, query);
        
        if base_score > 0.0 && cover_density > 0.0 {
            base_score * (1.0 + cover_density)
        } else {
            base_score
        }
    }
}

impl Default for TfIdfRanking {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_vector(texts: &[&str]) -> TsVector {
        let tokens: Vec<Token> = texts.iter().enumerate()
            .map(|(i, &text)| Token {
                text: text.to_string(),
                position: i + 1,
                weight: 1.0,
                type_: TokenType::Word,
            })
            .collect();
        
        let positions: Vec<usize> = tokens.iter().map(|t| t.position).collect();
        let weights: Vec<f32> = tokens.iter().map(|t| t.weight).collect();
        
        TsVector {
            tokens,
            positions,
            weights,
        }
    }

    fn create_test_query(terms: &[&str]) -> ProcessedQuery {
        let tokens: Vec<Token> = terms.iter().enumerate()
            .map(|(i, &text)| Token {
                text: text.to_string(),
                position: i + 1,
                weight: 1.0,
                type_: TokenType::Word,
            })
            .collect();
        
        let operators = if tokens.len() > 1 {
            vec![QueryOperator::And; tokens.len() - 1]
        } else {
            vec![]
        };

        ProcessedQuery { tokens, operators }
    }

    #[test]
    fn test_lexeme_weights() {
        let mut term_doc_frequencies = HashMap::new();
        term_doc_frequencies.insert("important".to_string(), 1);
        
        let ranking = BM25Ranking::with_collection_stats(1, term_doc_frequencies, 0.0);
        let mut config = RankingConfig::default();
        config.use_lexeme_weights = true;

        let doc = create_test_vector(&["important", "less", "important", "text"]);
        let query = create_test_query(&["important"]);

        let score_with_weights = ranking.rank(&doc, &query, &config);

        config.use_lexeme_weights = false;
        let score_without_weights = ranking.rank(&doc, &query, &config);

        assert!(score_with_weights > score_without_weights, 
            "Score with lexeme weights should be higher due to early position weight");
    }

    #[test]
    fn test_idf_scoring() {
        let mut term_doc_frequencies = HashMap::new();
        term_doc_frequencies.insert("common".to_string(), 100);
        term_doc_frequencies.insert("rare".to_string(), 2);
        
        let ranking = BM25Ranking::with_collection_stats(1000, term_doc_frequencies, 0.0);
        let config = RankingConfig::default();

        let doc1 = create_test_vector(&["common", "word"]);
        let doc2 = create_test_vector(&["rare", "word"]);
        
        let query1 = create_test_query(&["common"]);
        let query2 = create_test_query(&["rare"]);

        let common_score = ranking.rank(&doc1, &query1, &config);
        let rare_score = ranking.rank(&doc2, &query2, &config);

        assert!(rare_score > common_score, 
            "Rare terms should have higher IDF scores");
    }

    #[test]
    fn test_proximity_weighting() {
        let mut term_doc_frequencies = HashMap::new();
        term_doc_frequencies.insert("quick".to_string(), 1);
        term_doc_frequencies.insert("fox".to_string(), 1);
        
        let ranking = BM25Ranking::with_collection_stats(1, term_doc_frequencies, 0.0);
        let config = RankingConfig::default();

        // Test phrases with different proximities
        let close_doc = create_test_vector(&["quick", "brown", "fox"]);
        let far_doc = create_test_vector(&["quick", "very", "very", "very", "fox"]);
        
        let query = create_test_query(&["quick", "fox"]);

        let close_score = ranking.rank_cd(&close_doc, &query, &config);
        let far_score = ranking.rank_cd(&far_doc, &query, &config);

        assert!(close_score > far_score, 
            "Terms in closer proximity should score higher");
    }

    #[test]
    fn test_combined_features() {
        let mut term_doc_frequencies = HashMap::new();
        term_doc_frequencies.insert("rare".to_string(), 1);
        term_doc_frequencies.insert("common".to_string(), 100);
        
        let ranking = BM25Ranking::with_collection_stats(1000, term_doc_frequencies, 0.0);
        let config = RankingConfig::default();

        // Document with rare term in important position
        let doc1 = create_test_vector(&["rare", "text"]);
        // Document with common term in less important position
        let doc2 = create_test_vector(&["filler", "filler", "common"]);

        let query1 = create_test_query(&["rare"]);
        let query2 = create_test_query(&["common"]);

        let rare_important_score = ranking.rank(&doc1, &query1, &config);
        let common_unimportant_score = ranking.rank(&doc2, &query2, &config);

        assert!(rare_important_score > common_unimportant_score,
            "Rare terms in important positions should score highest");
    }

    #[test]
    fn test_bm25_ranking() {
        let mut term_doc_frequencies = HashMap::new();
        term_doc_frequencies.insert("test".to_string(), 5);
        term_doc_frequencies.insert("document".to_string(), 3);
        
        let avg_doc_length = 20.0;
        let ranking = BM25Ranking::with_collection_stats(10, term_doc_frequencies, avg_doc_length);
        
        let mut config = RankingConfig::default();
        config.bm25_params = Some(BM25Params::default());

        // Create a test document
        let doc = create_test_vector(&["test", "document", "test"]);
        let query = create_test_query(&["test"]);

        let score = ranking.rank(&doc, &query, &config);
        assert!(score > 0.0);

        // Test with different k1 values
        let mut config_high_k1 = config.clone();
        config_high_k1.bm25_params = Some(BM25Params { k1: 2.0, b: 0.75 });
        let score_high_k1 = ranking.rank(&doc, &query, &config_high_k1);

        let mut config_low_k1 = config.clone();
        config_low_k1.bm25_params = Some(BM25Params { k1: 1.0, b: 0.75 });
        let score_low_k1 = ranking.rank(&doc, &query, &config_low_k1);

        // Higher k1 should give more weight to term frequency
        assert!(score_high_k1 > score_low_k1);
    }

    #[test]
    fn test_bm25_document_length_normalization() {
        let mut term_doc_frequencies = HashMap::new();
        term_doc_frequencies.insert("test".to_string(), 5);
        
        let avg_doc_length = 10.0;
        let ranking = BM25Ranking::with_collection_stats(10, term_doc_frequencies, avg_doc_length);
        
        let mut config = RankingConfig::default();
        config.bm25_params = Some(BM25Params::default());

        // Create two documents with same term frequency but different lengths
        let short_doc = create_test_vector(&["test", "test", "short"]);
        let long_doc = create_test_vector(&["test", "test", "very", "long", "document", "with", "more", "words"]);
        let query = create_test_query(&["test"]);

        let score_short = ranking.rank(&short_doc, &query, &config);
        let score_long = ranking.rank(&long_doc, &query, &config);

        // Shorter document should get higher score due to length normalization
        assert!(score_short > score_long);

        // Test with no length normalization (b = 0)
        let mut config_no_length_norm = config.clone();
        config_no_length_norm.bm25_params = Some(BM25Params { k1: 1.5, b: 0.0 });
        
        let score_short_no_norm = ranking.rank(&short_doc, &query, &config_no_length_norm);
        let score_long_no_norm = ranking.rank(&long_doc, &query, &config_no_length_norm);

        // Scores should be closer when length normalization is disabled
        let diff_with_norm = (score_short - score_long).abs();
        let diff_without_norm = (score_short_no_norm - score_long_no_norm).abs();
        assert!(diff_without_norm < diff_with_norm);
    }

    #[test]
    fn test_tfidf_normalization() {
        let mut term_doc_frequencies = HashMap::new();
        term_doc_frequencies.insert("test".to_string(), 5);
        
        let ranking = TfIdfRanking::with_collection_stats(10, term_doc_frequencies, 10.0);
        let config = RankingConfig::default();

        let doc = create_test_vector(&["test", "test", "document"]);
        let query = create_test_query(&["test"]);

        let score = ranking.rank(&doc, &query, &config);
        assert!(score > 0.0, "TF-IDF score should be positive");
    }

    #[test]
    fn test_tfidf_length_penalty() {
        let mut term_doc_frequencies = HashMap::new();
        term_doc_frequencies.insert("test".to_string(), 5);
        
        let ranking = TfIdfRanking::with_collection_stats(10, term_doc_frequencies, 5.0);
        let config = RankingConfig::default();

        // Create two documents with same term frequency but different lengths
        let short_doc = create_test_vector(&["test", "test"]);
        let long_doc = create_test_vector(&["test", "test", "padding", "words", "here"]);
        let query = create_test_query(&["test"]);

        let score_short = ranking.rank(&short_doc, &query, &config);
        let score_long = ranking.rank(&long_doc, &query, &config);

        assert!(score_short > score_long, "Shorter document should score higher with length penalty");
    }
} 