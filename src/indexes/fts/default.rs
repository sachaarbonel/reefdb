use super::{memory::InvertedIndex, tokenizers::default::DefaultTokenizer, disk::OnDiskInvertedIndex};

pub type DefaultSearchIdx = InvertedIndex<DefaultTokenizer>;

pub type OnDiskSearchIdx = OnDiskInvertedIndex<DefaultTokenizer>;
