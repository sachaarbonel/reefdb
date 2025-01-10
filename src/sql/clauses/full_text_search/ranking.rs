#[derive(Debug, Clone, PartialEq)]
pub struct TSRanking {
    pub weights: Option<[f32; 4]>,  // D, C, B, A weights
    pub normalization: u32,         // Bit mask for normalization options
}

impl TSRanking {
    pub fn new() -> Self {
        Self {
            weights: None,
            normalization: 0,
        }
    }

    pub fn with_weights(mut self, weights: [f32; 4]) -> Self {
        self.weights = Some(weights);
        self
    }

    pub fn with_normalization(mut self, normalization: u32) -> Self {
        self.normalization = normalization;
        self
    }
}

impl Default for TSRanking {
    fn default() -> Self {
        Self::new()
    }
}

// Normalization options
pub const NORM_LENGTH: u32 = 0x01;         // Divide by document length
pub const NORM_UNIQUE_WORDS: u32 = 0x02;   // Divide by number of unique words
pub const NORM_LOG_LENGTH: u32 = 0x04;     // Take log of document length
pub const NORM_WORD_COUNT: u32 = 0x08;     // Divide by 1 + log of total words
pub const NORM_UNIQUE_COUNT: u32 = 0x10;   // Divide by 1 + log of unique words
pub const NORM_RANK: u32 = 0x20;           // Divide by 1 + rank
pub const NORM_UPPER_BOUND: u32 = 0x40;    // Divide by 1 + rank / upper bound 

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_ranking() {
        let ranking = TSRanking::new();
        assert_eq!(ranking.weights, None);
        assert_eq!(ranking.normalization, 0);
    }

    #[test]
    fn test_with_weights() {
        let weights = [0.1, 0.2, 0.4, 0.8];
        let ranking = TSRanking::new().with_weights(weights);
        assert_eq!(ranking.weights, Some(weights));
        assert_eq!(ranking.normalization, 0);
    }

    #[test]
    fn test_with_normalization() {
        let ranking = TSRanking::new()
            .with_normalization(NORM_LENGTH | NORM_UNIQUE_WORDS);
        assert_eq!(ranking.weights, None);
        assert_eq!(ranking.normalization, NORM_LENGTH | NORM_UNIQUE_WORDS);
    }

    #[test]
    fn test_default() {
        let ranking = TSRanking::default();
        assert_eq!(ranking.weights, None);
        assert_eq!(ranking.normalization, 0);
    }

    #[test]
    fn test_chaining() {
        let weights = [0.1, 0.2, 0.4, 0.8];
        let ranking = TSRanking::new()
            .with_weights(weights)
            .with_normalization(NORM_LENGTH);
        assert_eq!(ranking.weights, Some(weights));
        assert_eq!(ranking.normalization, NORM_LENGTH);
    }
}