//! Kubernetes [`Duration`]s.
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use std::{cmp::Ordering, fmt, str::FromStr, time};

/// A Kubernetes duration.
///
/// This is equivalent to the [`metav1.Duration`] type in the Go Kubernetes
/// apimachinery package. A [`metav1.Duration`] is serialized in YAML and JSON
/// as a string formatted in the format accepted by the Go standard library's
/// [`time.ParseDuration()`] function. This type is a similar wrapper around
/// Rust's [`std::time::Duration`] that can be serialized and deserialized using
/// the same format as `metav1.Duration`.
///
/// # On Signedness
///
/// Go's [`time.Duration`] type is a signed integer type, while Rust's
/// [`std::time::Duration`] is unsigned. Therefore, this type is also capable of
/// representing both positive and negative durations. This is implemented by
/// storing whether or not the parsed duration was negative as a boolean field
/// in the wrapper type. The [`Duration::is_negative`] method returns this
/// value, and when a [`Duration`] is serialized, the negative sign is included
/// if the duration is negative.
///
/// [`Duration`]s can be compared with [`std::time::Duration`]s. If the
/// [`Duration`] is negative, it will always be considered less than the
/// [`std::time::Duration`]. Similarly, because [`std::time::Duration`]s are
/// unsigned, a negative [`Duration`] will never be equal to a
/// [`std::time::Duration`], even if the wrapped [`std::time::Duration`] (the
/// negative duration's absolute value) is equal.
///
/// When converting a [`Duration`] into a [`std::time::Duration`], be aware that
/// *this information is lost*: if a negative [`Duration`] is converted into a
/// [`std::time::Duration`] and then that [`std::time::Duration`] is converted
/// back into a [`Duration`], the second [`Duration`] will *not* be negative.
///
/// [`metav1.Duration`]: https://pkg.go.dev/k8s.io/apimachinery/pkg/apis/meta/v1#Duration
/// [`time.Duration`]: https://pkg.go.dev/time#Duration
/// [`time.ParseDuration()`]: https://pkg.go.dev/time#ParseDuration
#[derive(Copy, Clone, PartialEq, Eq)]
pub struct Duration {
    duration: time::Duration,
    is_negative: bool,
}

/// Errors returned by the [`FromStr`] implementation for [`Duration`].

#[derive(Debug, thiserror::Error, Eq, PartialEq)]
#[non_exhaustive]
pub enum ParseError {
    /// An invalid unit was provided. Units must be one of 'ns', 'us', 'μs',
    /// 's', 'ms', 's', 'm', or 'h'.
    #[error("invalid unit: {}", EXPECTED_UNITS)]
    InvalidUnit,

    /// No unit was provided.
    #[error("missing a unit: {}", EXPECTED_UNITS)]
    NoUnit,

    /// The number associated with a given unit was invalid.
    #[error("invalid floating-point number: {}", .0)]
    NotANumber(#[from] std::num::ParseFloatError),
}

const EXPECTED_UNITS: &str = "expected one of 'ns', 'us', '\u{00b5}s', 'ms', 's', 'm', or 'h'";

impl From<time::Duration> for Duration {
    fn from(duration: time::Duration) -> Self {
        Self {
            duration,
            is_negative: false,
        }
    }
}

impl From<Duration> for time::Duration {
    fn from(Duration { duration, .. }: Duration) -> Self {
        duration
    }
}

impl Duration {
    /// Returns `true` if this `Duration` is negative.
    #[inline]
    #[must_use]
    pub fn is_negative(&self) -> bool {
        self.is_negative
    }
}

impl fmt::Debug for Duration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use std::fmt::Write;
        if self.is_negative {
            f.write_char('-')?;
        }
        fmt::Debug::fmt(&self.duration, f)
    }
}

impl fmt::Display for Duration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use std::fmt::Write;
        if self.is_negative {
            f.write_char('-')?;
        }
        fmt::Debug::fmt(&self.duration, f)
    }
}

impl FromStr for Duration {
    type Err = ParseError;

    fn from_str(mut s: &str) -> Result<Self, Self::Err> {
        // implements the same format as
        // https://cs.opensource.google/go/go/+/refs/tags/go1.20.4:src/time/format.go;l=1589
        const MINUTE: time::Duration = time::Duration::from_secs(60);

        // Go durations are signed. Rust durations aren't.
        let is_negative = s.starts_with('-');
        s = s.trim_start_matches('+').trim_start_matches('-');

        let mut total = time::Duration::from_secs(0);
        while !s.is_empty() && s != "0" {
            let unit_start = s.find(|c: char| c.is_alphabetic()).ok_or(ParseError::NoUnit)?;

            let (val, rest) = s.split_at(unit_start);
            let val = val.parse::<f64>()?;
            let unit = if let Some(next_numeric_start) = rest.find(|c: char| !c.is_alphabetic()) {
                let (unit, rest) = rest.split_at(next_numeric_start);
                s = rest;
                unit
            } else {
                s = "";
                rest
            };

            // https://cs.opensource.google/go/go/+/refs/tags/go1.20.4:src/time/format.go;l=1573
            let base = match unit {
                "ns" => time::Duration::from_nanos(1),
                // U+00B5 is the "micro sign" while U+03BC is "Greek letter mu"
                "us" | "\u{00b5}s" | "\u{03bc}s" => time::Duration::from_micros(1),
                "ms" => time::Duration::from_millis(1),
                "s" => time::Duration::from_secs(1),
                "m" => MINUTE,
                "h" => MINUTE * 60,
                _ => return Err(ParseError::InvalidUnit),
            };

            total += base.mul_f64(val);
        }

        Ok(Duration {
            duration: total,
            is_negative,
        })
    }
}

impl Serialize for Duration {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for Duration {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct Visitor;
        impl de::Visitor<'_> for Visitor {
            type Value = Duration;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a string in Go `time.Duration.String()` format")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                let val = value.parse::<Duration>().map_err(de::Error::custom)?;
                Ok(val)
            }
        }
        deserializer.deserialize_str(Visitor)
    }
}

impl PartialEq<time::Duration> for Duration {
    fn eq(&self, other: &time::Duration) -> bool {
        // Since `std::time::Duration` is unsigned, a negative `Duration` is
        // never equal to a `std::time::Duration`.
        if self.is_negative {
            return false;
        }

        self.duration == *other
    }
}

impl PartialEq<time::Duration> for &'_ Duration {
    fn eq(&self, other: &time::Duration) -> bool {
        // Since `std::time::Duration` is unsigned, a negative `Duration` is
        // never equal to a `std::time::Duration`.
        if self.is_negative {
            return false;
        }

        self.duration == *other
    }
}

impl PartialEq<Duration> for time::Duration {
    fn eq(&self, other: &Duration) -> bool {
        // Since `std::time::Duration` is unsigned, a negative `Duration` is
        // never equal to a `std::time::Duration`.
        if other.is_negative {
            return false;
        }

        self == &other.duration
    }
}

impl PartialEq<Duration> for &'_ time::Duration {
    fn eq(&self, other: &Duration) -> bool {
        // Since `std::time::Duration` is unsigned, a negative `Duration` is
        // never equal to a `std::time::Duration`.
        if other.is_negative {
            return false;
        }

        *self == &other.duration
    }
}

impl PartialOrd for Duration {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Duration {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self.is_negative, other.is_negative) {
            (true, false) => Ordering::Less,
            (false, true) => Ordering::Greater,
            // if both durations are negative, the "higher" Duration value is
            // actually the lower one
            (true, true) => self.duration.cmp(&other.duration).reverse(),
            (false, false) => self.duration.cmp(&other.duration),
        }
    }
}

impl PartialOrd<time::Duration> for Duration {
    fn partial_cmp(&self, other: &time::Duration) -> Option<Ordering> {
        // Since `std::time::Duration` is unsigned, a negative `Duration` is
        // always less than the `std::time::Duration`.
        if self.is_negative {
            return Some(Ordering::Less);
        }

        self.duration.partial_cmp(other)
    }
}

#[cfg(feature = "schema")]
impl schemars::JsonSchema for Duration {
    // see
    // https://github.com/kubernetes/apimachinery/blob/756e2227bf3a486098f504af1a0ffb736ad16f4c/pkg/apis/meta/v1/duration.go#L61
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "Duration"
    }

    fn is_referenceable() -> bool {
        false
    }

    fn json_schema(_: &mut schemars::generate::SchemaGenerator) -> schemars::schema::Schema {
        schemars::schema::SchemaObject {
            instance_type: Some(schemars::schema::InstanceType::String.into()),
            // the format should *not* be "duration", because "duration" means
            // the duration is formatted in ISO 8601, as described here:
            // https://datatracker.ietf.org/doc/html/draft-handrews-json-schema-validation-02#section-7.3.1
            format: None,
            ..Default::default()
        }
        .into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_the_same_as_go() {
        const MINUTE: time::Duration = time::Duration::from_secs(60);
        const HOUR: time::Duration = time::Duration::from_secs(60 * 60);
        // from Go:
        // https://cs.opensource.google/go/go/+/refs/tags/go1.20.4:src/time/time_test.go;l=891-951
        // ```
        // var parseDurationTests = []struct {
        // 	in   string
        // 	want time::Duration
        // }{
        let cases: &[(&str, Duration)] = &[
            // 	// simple
            // 	{"0", 0},
            ("0", time::Duration::from_secs(0).into()),
            // 	{"5s", 5 * Second},
            ("5s", time::Duration::from_secs(5).into()),
            // 	{"30s", 30 * Second},
            ("30s", time::Duration::from_secs(30).into()),
            // 	{"1478s", 1478 * Second},
            ("1478s", time::Duration::from_secs(1478).into()),
            // 	// sign
            // 	{"-5s", -5 * Second},
            ("-5s", Duration {
                duration: time::Duration::from_secs(5),
                is_negative: true,
            }),
            // 	{"+5s", 5 * Second},
            ("+5s", time::Duration::from_secs(5).into()),
            // 	{"-0", 0},
            ("-0", Duration {
                duration: time::Duration::from_secs(0),
                is_negative: true,
            }),
            // 	{"+0", 0},
            ("+0", time::Duration::from_secs(0).into()),
            // 	// decimal
            // 	{"5.0s", 5 * Second},
            ("5s", time::Duration::from_secs(5).into()),
            // 	{"5.6s", 5*Second + 600*Millisecond},
            (
                "5.6s",
                (time::Duration::from_secs(5) + time::Duration::from_millis(600)).into(),
            ),
            // 	{"5.s", 5 * Second},
            ("5.s", time::Duration::from_secs(5).into()),
            // 	{".5s", 500 * Millisecond},
            (".5s", time::Duration::from_millis(500).into()),
            // 	{"1.0s", 1 * Second},
            ("1.0s", time::Duration::from_secs(1).into()),
            // 	{"1.00s", 1 * Second},
            ("1.00s", time::Duration::from_secs(1).into()),
            // 	{"1.004s", 1*Second + 4*Millisecond},
            (
                "1.004s",
                (time::Duration::from_secs(1) + time::Duration::from_millis(4)).into(),
            ),
            // 	{"1.0040s", 1*Second + 4*Millisecond},
            (
                "1.0040s",
                (time::Duration::from_secs(1) + time::Duration::from_millis(4)).into(),
            ),
            // 	{"100.00100s", 100*Second + 1*Millisecond},
            (
                "100.00100s",
                (time::Duration::from_secs(100) + time::Duration::from_millis(1)).into(),
            ),
            // 	// different units
            // 	{"10ns", 10 * Nanosecond},
            ("10ns", time::Duration::from_nanos(10).into()),
            // 	{"11us", 11 * Microsecond},
            ("11us", time::Duration::from_micros(11).into()),
            // 	{"12µs", 12 * Microsecond}, // U+00B5
            ("12µs", time::Duration::from_micros(12).into()),
            // 	{"12μs", 12 * Microsecond}, // U+03BC
            ("12μs", time::Duration::from_micros(12).into()),
            // 	{"13ms", 13 * Millisecond},
            ("13ms", time::Duration::from_millis(13).into()),
            // 	{"14s", 14 * Second},
            ("14s", time::Duration::from_secs(14).into()),
            // 	{"15m", 15 * Minute},
            ("15m", (15 * MINUTE).into()),
            // 	{"16h", 16 * Hour},
            ("16h", (16 * HOUR).into()),
            // 	// composite durations
            // 	{"3h30m", 3*Hour + 30*Minute},
            ("3h30m", (3 * HOUR + 30 * MINUTE).into()),
            // 	{"10.5s4m", 4*Minute + 10*Second + 500*Millisecond},
            (
                "10.5s4m",
                (4 * MINUTE + time::Duration::from_secs(10) + time::Duration::from_millis(500)).into(),
            ),
            // 	{"-2m3.4s", -(2*Minute + 3*Second + 400*Millisecond)},
            ("-2m3.4s", Duration {
                duration: 2 * MINUTE + time::Duration::from_secs(3) + time::Duration::from_millis(400),
                is_negative: true,
            }),
            // 	{"1h2m3s4ms5us6ns", 1*Hour + 2*Minute + 3*Second + 4*Millisecond + 5*Microsecond + 6*Nanosecond},
            (
                "1h2m3s4ms5us6ns",
                (1 * HOUR
                    + 2 * MINUTE
                    + time::Duration::from_secs(3)
                    + time::Duration::from_millis(4)
                    + time::Duration::from_micros(5)
                    + time::Duration::from_nanos(6))
                .into(),
            ),
            // 	{"39h9m14.425s", 39*Hour + 9*Minute + 14*Second + 425*Millisecond},
            (
                "39h9m14.425s",
                (39 * HOUR + 9 * MINUTE + time::Duration::from_secs(14) + time::Duration::from_millis(425))
                    .into(),
            ),
            // 	// large value
            // 	{"52763797000ns", 52763797000 * Nanosecond},
            ("52763797000ns", time::Duration::from_nanos(52763797000).into()),
            // 	// more than 9 digits after decimal point, see https://golang.org/issue/6617
            // 	{"0.3333333333333333333h", 20 * Minute},
            ("0.3333333333333333333h", (20 * MINUTE).into()),
            // 	// 9007199254740993 = 1<<53+1 cannot be stored precisely in a float64
            // 	{"9007199254740993ns", (1<<53 + 1) * Nanosecond},
            (
                "9007199254740993ns",
                time::Duration::from_nanos((1 << 53) + 1).into(),
            ),
            // Rust Durations can handle larger durations than Go's
            // representation, so skip these tests for their precision limits

            // 	// largest duration that can be represented by int64 in nanoseconds
            // 	{"9223372036854775807ns", (1<<63 - 1) * Nanosecond},
            // ("9223372036854775807ns", time::Duration::from_nanos((1 << 63) - 1).into()),
            // 	{"9223372036854775.807us", (1<<63 - 1) * Nanosecond},
            // ("9223372036854775.807us", time::Duration::from_nanos((1 << 63) - 1).into()),
            // 	{"9223372036s854ms775us807ns", (1<<63 - 1) * Nanosecond},
            // 	{"-9223372036854775808ns", -1 << 63 * Nanosecond},
            // 	{"-9223372036854775.808us", -1 << 63 * Nanosecond},
            // 	{"-9223372036s854ms775us808ns", -1 << 63 * Nanosecond},
            // 	// largest negative value
            // 	{"-9223372036854775808ns", -1 << 63 * Nanosecond},
            // 	// largest negative round trip value, see https://golang.org/issue/48629
            // 	{"-2562047h47m16.854775808s", -1 << 63 * Nanosecond},

            // 	// huge string; issue 15011.
            // 	{"0.100000000000000000000h", 6 * Minute},
            ("0.100000000000000000000h", (6 * MINUTE).into()), // 	// This value tests the first overflow check in leadingFraction.
                                                               // 	{"0.830103483285477580700h", 49*Minute + 48*Second + 372539827*Nanosecond},
                                                               // }
                                                               // ```
        ];

        for (input, expected) in cases {
            let parsed = dbg!(input).parse::<Duration>().unwrap();
            assert_eq!(&dbg!(parsed), expected);
        }
    }
}
