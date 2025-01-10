#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TextWeight {
    D = 0,
    C = 1,
    B = 2,
    A = 3,
}

impl TextWeight {
    pub fn from_char(c: char) -> Option<Self> {
        match c.to_ascii_uppercase() {
            'A' => Some(TextWeight::A),
            'B' => Some(TextWeight::B),
            'C' => Some(TextWeight::C),
            'D' => Some(TextWeight::D),
            _ => None,
        }
    }

    pub fn to_f32(self) -> f32 {
        match self {
            TextWeight::D => 0.1,
            TextWeight::C => 0.2,
            TextWeight::B => 0.4,
            TextWeight::A => 1.0,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct WeightedTsVector {
    pub text: String,
    pub weight: TextWeight,
}

impl WeightedTsVector {
    pub fn new(text: String, weight: TextWeight) -> Self {
        Self { text, weight }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_weight_from_char() {
        assert_eq!(TextWeight::from_char('A'), Some(TextWeight::A));
        assert_eq!(TextWeight::from_char('B'), Some(TextWeight::B));
        assert_eq!(TextWeight::from_char('C'), Some(TextWeight::C));
        assert_eq!(TextWeight::from_char('D'), Some(TextWeight::D));
        assert_eq!(TextWeight::from_char('a'), Some(TextWeight::A));
        assert_eq!(TextWeight::from_char('X'), None);
    }

    #[test]
    fn test_weight_to_f32() {
        assert_eq!(TextWeight::A.to_f32(), 1.0);
        assert_eq!(TextWeight::B.to_f32(), 0.4);
        assert_eq!(TextWeight::C.to_f32(), 0.2);
        assert_eq!(TextWeight::D.to_f32(), 0.1);
    }
} 