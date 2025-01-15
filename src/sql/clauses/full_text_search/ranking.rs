use super::types::ParseError;

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

    pub fn parse(input: &str) -> Result<Self, ParseError> {
        // Parse ts_rank function call: ts_rank([weights], tsvector, tsquery [, normalization])
        let input = input.trim();
        
        if !input.starts_with("ts_rank(") || !input.ends_with(")") {
            return Err(ParseError::InvalidSyntax("Expected ts_rank function call".to_string()));
        }

        // Extract arguments between parentheses and split by comma
        let args_str = &input[8..input.len()-1];  // Remove ts_rank( and )
        let mut args = Vec::new();
        let mut current_arg = String::new();
        let mut bracket_count = 0;
        
        for c in args_str.chars() {
            match c {
                '[' => {
                    bracket_count += 1;
                    current_arg.push(c);
                }
                ']' => {
                    bracket_count -= 1;
                    current_arg.push(c);
                }
                ',' if bracket_count == 0 => {
                    if !current_arg.trim().is_empty() {
                        args.push(current_arg.trim().to_string());
                    }
                    current_arg = String::new();
                }
                _ => {
                    current_arg.push(c);
                }
            }
        }
        if !current_arg.trim().is_empty() {
            args.push(current_arg.trim().to_string());
        }

        match args.len() {
            2 => {
                // ts_rank(tsvector, tsquery)
                Ok(TSRanking::new())
            },
            3 => {
                // Could be either:
                // ts_rank([D,C,B,A], tsvector, tsquery) or
                // ts_rank(tsvector, tsquery, normalization)
                if args[0].starts_with('[') && args[0].ends_with(']') {
                    // Parse weights
                    let weights = Self::parse_weights(&args[0])?;
                    Ok(TSRanking::new().with_weights(weights))
                } else {
                    // Parse normalization
                    let normalization = args[2].parse::<u32>()
                        .map_err(|_| ParseError::InvalidSyntax("Invalid normalization value".to_string()))?;
                    Ok(TSRanking::new().with_normalization(normalization))
                }
            },
            4 => {
                // ts_rank([D,C,B,A], tsvector, tsquery, normalization)
                let weights = Self::parse_weights(&args[0])?;
                let normalization = args[3].parse::<u32>()
                    .map_err(|_| ParseError::InvalidSyntax("Invalid normalization value".to_string()))?;
                Ok(TSRanking::new()
                    .with_weights(weights)
                    .with_normalization(normalization))
            },
            _ => Err(ParseError::InvalidSyntax("Invalid number of arguments".to_string()))
        }
    }

    fn parse_weights(weights_str: &str) -> Result<[f32; 4], ParseError> {
        let weights_str = weights_str.trim();
        if !weights_str.starts_with('[') || !weights_str.ends_with(']') {
            return Err(ParseError::InvalidSyntax("Weights must be enclosed in []".to_string()));
        }

        let weights: Vec<f32> = weights_str[1..weights_str.len()-1]
            .split(',')
            .map(|w| w.trim().parse::<f32>())
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| ParseError::InvalidSyntax("Invalid weight value".to_string()))?;

        if weights.len() != 4 {
            return Err(ParseError::InvalidSyntax("Expected 4 weights [D,C,B,A]".to_string()));
        }

        Ok([weights[0], weights[1], weights[2], weights[3]])
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

    #[test]
    fn test_parse_basic() {
        let ranking = TSRanking::parse("ts_rank(vector, query)").unwrap();
        assert_eq!(ranking.weights, None);
        assert_eq!(ranking.normalization, 0);
    }

    #[test]
    fn test_parse_with_weights() {
        let ranking = TSRanking::parse("ts_rank([0.1, 0.2, 0.4, 1.0], vector, query)").unwrap();
        assert_eq!(ranking.weights, Some([0.1, 0.2, 0.4, 1.0]));
        assert_eq!(ranking.normalization, 0);
    }

    #[test]
    fn test_parse_with_normalization() {
        let ranking = TSRanking::parse("ts_rank(vector, query, 4)").unwrap();
        assert_eq!(ranking.weights, None);
        assert_eq!(ranking.normalization, 4);
    }

    #[test]
    fn test_parse_full() {
        let ranking = TSRanking::parse("ts_rank([0.1, 0.2, 0.4, 1.0], vector, query, 4)").unwrap();
        assert_eq!(ranking.weights, Some([0.1, 0.2, 0.4, 1.0]));
        assert_eq!(ranking.normalization, 4);
    }

    #[test]
    fn test_parse_invalid() {
        assert!(TSRanking::parse("invalid").is_err());
        assert!(TSRanking::parse("ts_rank()").is_err());
        assert!(TSRanking::parse("ts_rank([0.1], vector, query)").is_err());
        assert!(TSRanking::parse("ts_rank([0.1, 0.2, 0.4], vector, query)").is_err());
        assert!(TSRanking::parse("ts_rank(vector, query, invalid)").is_err());
    }
}