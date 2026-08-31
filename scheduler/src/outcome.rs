use time::OffsetDateTime;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Done(String),
    Retry,
    RetryAt(OffsetDateTime),
    GiveUp(String),
}

impl Outcome {
    pub fn done(business_status: impl Into<String>) -> Self {
        Self::Done(business_status.into())
    }

    pub fn give_up(business_status: impl Into<String>) -> Self {
        Self::GiveUp(business_status.into())
    }

    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Done(_) | Self::GiveUp(_))
    }
}
