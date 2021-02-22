package properties

//// OUTPUTS QUEUES AND EXCHANGES
// Aggregators
const AggregatorA1_Output = "A1-to-J1"
const AggregatorA2_Output = "A2-to-A3"
const AggregatorA3_Output = "A3-to-P1"
const AggregatorA4_Output = "A4-to-P2"
const AggregatorA5_Output = "A5-to-P3"
const AggregatorA7_Output = "A7-to-F4"
const AggregatorA8_Output = "A8-to-J3"

// Filters
const FilterF1_Output = "F1-to-A1"
const FilterF3_Output = "F3-to-J2"
const FilterF4_Output1 = "F4-to-P4"
const FilterF4_Output2 = "F4-to-J3"
const FilterF5_Output = "F5-to-A8"

// Inputs
const InputI1_Output = "I1-to-M1"
const InputI2_Output = "I2.0-to-M2,M3,M4,M5,M6"
const InputI3_Output = "I2.1-to-M2,M3,M4,M5,M6"

// Joiners
const JoinerJ1_Output = "J1-to-A2"
const JoinerJ2_Output = "J2-to-P3"
const JoinerJ3_Output = "J3-to-P5"

// Mappers
const MapperM1_Output = "M1-to-J1"
const MapperM2_Output = "M2-to-F1"
const MapperM3_Output = "M3-to-A4"
const MapperM4_Output = "M4-to-A5"
const MapperM5_Output = "M5-to-A7"
const MapperM6_Output = "M6-to-F5"

// Prettiers
const PrettierP1_Output = "P1-to-O1"
const PrettierP2_Output = "P2-to-O1"
const PrettierP3_Output = "P3-to-O1"
const PrettierP4_Output = "P4-to-O1"
const PrettierP5_Output = "P5-to-O1"


//// EXCHANGES INNER QUEUES
// Aggregators
const AggregatorA1_Input = "A1-from-F1"
const AggregatorA2_Input = "A2-from-J1"
const AggregatorA3_Input = "A3-from-A2"
const AggregatorA4_Input = "A4-from-M3"
const AggregatorA5_Input = "A5-from-M4"
const AggregatorA7_Input = "A7-from-M5"
const AggregatorA8_Input = "A8-from-F5"

// Joiners
const JoinerJ1_Input1 = "J1-from-M1"
const JoinerJ1_Input2 = "J1-from-A1"
const JoinerJ3_Input1 = "J3-from-F4"
const JoinerJ3_Input2 = "J3-from-A8"

// Mappers
const MapperM2_Input = "M2-from-I2.0"
const MapperM3_Input = "M3-from-I2.0"
const MapperM4_Input = "M4-from-I2.0"
const MapperM5_Input = "M5-from-I2.0"
const MapperM6_Input = "M6-from-I2.0"
