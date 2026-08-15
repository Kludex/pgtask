use std::{fmt, num::NonZeroU32, str::FromStr};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

const MAX_QUEUE_NAME_BYTES: usize = 128;
const MAX_TASK_NAME_BYTES: usize = 255;

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum NameError {
    #[error("{kind} must not be empty")]
    Empty { kind: &'static str },
    #[error("{kind} must be at most {maximum} bytes, got {actual}")]
    TooLong {
        kind: &'static str,
        maximum: usize,
        actual: usize,
    },
    #[error("{kind} contains unsupported character {character:?}")]
    UnsupportedCharacter { kind: &'static str, character: char },
}

fn validate_name(value: &str, kind: &'static str, maximum: usize) -> Result<(), NameError> {
    if value.is_empty() {
        return Err(NameError::Empty { kind });
    }
    if value.len() > maximum {
        return Err(NameError::TooLong {
            kind,
            maximum,
            actual: value.len(),
        });
    }
    if let Some(character) = value
        .chars()
        .find(|character| !(character.is_ascii_alphanumeric() || matches!(character, '.' | '_' | ':' | '-')))
    {
        return Err(NameError::UnsupportedCharacter { kind, character });
    }
    Ok(())
}

macro_rules! name_type {
    ($name:ident, $kind:literal, $maximum:expr) => {
        #[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
        #[serde(try_from = "String", into = "String")]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self, NameError> {
                let value = value.into();
                validate_name(&value, $kind, $maximum)?;
                Ok(Self(value))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                self.as_str()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(self.as_str())
            }
        }

        impl TryFrom<String> for $name {
            type Error = NameError;

            fn try_from(value: String) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        impl From<$name> for String {
            fn from(value: $name) -> Self {
                value.0
            }
        }
    };
}

name_type!(QueueName, "queue name", MAX_QUEUE_NAME_BYTES);
name_type!(ScheduleName, "schedule name", MAX_TASK_NAME_BYTES);
name_type!(SignalName, "signal name", MAX_TASK_NAME_BYTES);
name_type!(StepName, "step name", MAX_TASK_NAME_BYTES);
name_type!(TaskName, "task name", MAX_TASK_NAME_BYTES);

impl Default for QueueName {
    fn default() -> Self {
        Self("default".to_owned())
    }
}

macro_rules! uuid_type {
    ($name:ident) => {
        #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(Uuid);

        impl $name {
            pub fn new() -> Self {
                Self(Uuid::now_v7())
            }

            pub const fn from_uuid(value: Uuid) -> Self {
                Self(value)
            }

            pub const fn as_uuid(self) -> Uuid {
                self.0
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(formatter)
            }
        }

        impl FromStr for $name {
            type Err = uuid::Error;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Uuid::parse_str(value).map(Self)
            }
        }

        impl From<$name> for Uuid {
            fn from(value: $name) -> Self {
                value.0
            }
        }
    };
}

uuid_type!(TaskId);
uuid_type!(ScheduleId);
uuid_type!(WorkerId);
uuid_type!(LeaseToken);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct HandlerVersion(NonZeroU32);

impl HandlerVersion {
    pub const fn new(value: NonZeroU32) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u32 {
        self.0.get()
    }
}

impl Default for HandlerVersion {
    fn default() -> Self {
        Self(NonZeroU32::MIN)
    }
}
