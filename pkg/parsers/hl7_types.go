package parsers

import (
	"time"
)

// HL7MessageInterface represents any HL7 message
type HL7MessageInterface interface {
	GetMessageType() string
	GetMessageControlID() string
	GetTimestamp() time.Time
}

// Common composite data types

// HD - Hierarchic Designator
type HD struct {
	NamespaceID     string `json:"namespace_id"`      // HD.1
	UniversalID     string `json:"universal_id"`      // HD.2
	UniversalIDType string `json:"universal_id_type"` // HD.3
}

// MSG - Message Type
type MSG struct {
	MessageCode      string `json:"message_code"`      // MSG.1
	TriggerEvent     string `json:"trigger_event"`     // MSG.2
	MessageStructure string `json:"message_structure"` // MSG.3
}

// PT - Processing Type
type PT struct {
	ProcessingID   string `json:"processing_id"`   // PT.1
	ProcessingMode string `json:"processing_mode"` // PT.2
}

// VID - Version Identifier
type VID struct {
	VersionID                string `json:"version_id"`                // VID.1
	InternationalizationCode *CE    `json:"internationalization_code"` // VID.2
	InternationalVersionID   *CE    `json:"international_version_id"`  // VID.3
}

// EI - Entity Identifier
type EI struct {
	EntityIdentifier string `json:"entity_identifier"` // EI.1
	NamespaceID      string `json:"namespace_id"`      // EI.2
	UniversalID      string `json:"universal_id"`      // EI.3
	UniversalIDType  string `json:"universal_id_type"` // EI.4
}

// XPN - Extended Person Name
type XPN struct {
	FamilyName                 string    `json:"family_name"`                    // XPN.1
	GivenName                  string    `json:"given_name"`                     // XPN.2
	SecondAndFurtherGivenNames string    `json:"second_and_further_given_names"` // XPN.3
	Suffix                     string    `json:"suffix"`                         // XPN.4
	Prefix                     string    `json:"prefix"`                         // XPN.5
	Degree                     string    `json:"degree"`                         // XPN.6
	NameTypeCode               string    `json:"name_type_code"`                 // XPN.7
	NameRepresentationCode     string    `json:"name_representation_code"`       // XPN.8
	NameContext                *CE       `json:"name_context"`                   // XPN.9
	NameValidityRange          string    `json:"name_validity_range"`            // XPN.10
	NameAssemblyOrder          string    `json:"name_assembly_order"`            // XPN.11
	EffectiveDate              time.Time `json:"effective_date"`                 // XPN.12
	ExpirationDate             time.Time `json:"expiration_date"`                // XPN.13
	ProfessionalSuffix         string    `json:"professional_suffix"`            // XPN.14
}

// XAD - Extended Address
type XAD struct {
	StreetAddress              string    `json:"street_address"`               // XAD.1
	OtherDesignation           string    `json:"other_designation"`            // XAD.2
	City                       string    `json:"city"`                         // XAD.3
	StateOrProvince            string    `json:"state_or_province"`            // XAD.4
	ZipOrPostalCode            string    `json:"zip_or_postal_code"`           // XAD.5
	Country                    string    `json:"country"`                      // XAD.6
	AddressType                string    `json:"address_type"`                 // XAD.7
	OtherGeographicDesignation string    `json:"other_geographic_designation"` // XAD.8
	CountyParishCode           string    `json:"county_parish_code"`           // XAD.9
	CensusTract                string    `json:"census_tract"`                 // XAD.10
	AddressRepresentationCode  string    `json:"address_representation_code"`  // XAD.11
	AddressValidityRange       string    `json:"address_validity_range"`       // XAD.12
	EffectiveDate              time.Time `json:"effective_date"`               // XAD.13
	ExpirationDate             time.Time `json:"expiration_date"`              // XAD.14
}

// XTN - Extended Telecommunications Number
type XTN struct {
	TelephoneNumber                string `json:"telephone_number"`                 // XTN.1
	TelecommunicationUseCode       string `json:"telecommunication_use_code"`       // XTN.2
	TelecommunicationEquipmentType string `json:"telecommunication_equipment_type"` // XTN.3
	EmailAddress                   string `json:"email_address"`                    // XTN.4
	CountryCode                    string `json:"country_code"`                     // XTN.5
	AreaCityCode                   string `json:"area_city_code"`                   // XTN.6
	LocalNumber                    string `json:"local_number"`                     // XTN.7
	Extension                      string `json:"extension"`                        // XTN.8
	AnyText                        string `json:"any_text"`                         // XTN.9
	ExtensionPrefix                string `json:"extension_prefix"`                 // XTN.10
	SpeedDialCode                  string `json:"speed_dial_code"`                  // XTN.11
	UnformattedTelephoneNumber     string `json:"unformatted_telephone_number"`     // XTN.12
}

// XCN - Extended Composite ID Number and Name for Persons
type XCN struct {
	IDNumber                    string    `json:"id_number"`                      // XCN.1
	FamilyName                  string    `json:"family_name"`                    // XCN.2
	GivenName                   string    `json:"given_name"`                     // XCN.3
	SecondAndFurtherGivenNames  string    `json:"second_and_further_given_names"` // XCN.4
	Suffix                      string    `json:"suffix"`                         // XCN.5
	Prefix                      string    `json:"prefix"`                         // XCN.6
	Degree                      string    `json:"degree"`                         // XCN.7
	SourceTable                 string    `json:"source_table"`                   // XCN.8
	AssigningAuthority          *HD       `json:"assigning_authority"`            // XCN.9
	NameTypeCode                string    `json:"name_type_code"`                 // XCN.10
	IdentifierCheckDigit        string    `json:"identifier_check_digit"`         // XCN.11
	CheckDigitScheme            string    `json:"check_digit_scheme"`             // XCN.12
	IdentifierTypeCode          string    `json:"identifier_type_code"`           // XCN.13
	AssigningFacility           *HD       `json:"assigning_facility"`             // XCN.14
	NameRepresentationCode      string    `json:"name_representation_code"`       // XCN.15
	NameContext                 *CE       `json:"name_context"`                   // XCN.16
	NameValidityRange           string    `json:"name_validity_range"`            // XCN.17
	NameAssemblyOrder           string    `json:"name_assembly_order"`            // XCN.18
	EffectiveDate               time.Time `json:"effective_date"`                 // XCN.19
	ExpirationDate              time.Time `json:"expiration_date"`                // XCN.20
	ProfessionalSuffix          string    `json:"professional_suffix"`            // XCN.21
	AssigningJurisdiction       *CWE      `json:"assigning_jurisdiction"`         // XCN.22
	AssigningAgencyOrDepartment *CWE      `json:"assigning_agency_or_department"` // XCN.23
}

// CE - Coded Element
type CE struct {
	Identifier                  string `json:"identifier"`                      // CE.1
	Text                        string `json:"text"`                            // CE.2
	NameOfCodingSystem          string `json:"name_of_coding_system"`           // CE.3
	AlternateIdentifier         string `json:"alternate_identifier"`            // CE.4
	AlternateText               string `json:"alternate_text"`                  // CE.5
	NameOfAlternateCodingSystem string `json:"name_of_alternate_coding_system"` // CE.6
}

// CWE - Coded with Exceptions
type CWE struct {
	Identifier                     string `json:"identifier"`                         // CWE.1
	Text                           string `json:"text"`                               // CWE.2
	NameOfCodingSystem             string `json:"name_of_coding_system"`              // CWE.3
	AlternateIdentifier            string `json:"alternate_identifier"`               // CWE.4
	AlternateText                  string `json:"alternate_text"`                     // CWE.5
	NameOfAlternateCodingSystem    string `json:"name_of_alternate_coding_system"`    // CWE.6
	CodingSystemVersionID          string `json:"coding_system_version_id"`           // CWE.7
	AlternateCodingSystemVersionID string `json:"alternate_coding_system_version_id"` // CWE.8
	OriginalText                   string `json:"original_text"`                      // CWE.9
}

// CX - Extended Composite ID with Check Digit
type CX struct {
	IDNumber                    string    `json:"id_number"`                      // CX.1
	CheckDigit                  string    `json:"check_digit"`                    // CX.2
	CheckDigitScheme            string    `json:"check_digit_scheme"`             // CX.3
	AssigningAuthority          *HD       `json:"assigning_authority"`            // CX.4
	IdentifierTypeCode          string    `json:"identifier_type_code"`           // CX.5
	AssigningFacility           *HD       `json:"assigning_facility"`             // CX.6
	EffectiveDate               time.Time `json:"effective_date"`                 // CX.7
	ExpirationDate              time.Time `json:"expiration_date"`                // CX.8
	AssigningJurisdiction       *CWE      `json:"assigning_jurisdiction"`         // CX.9
	AssigningAgencyOrDepartment *CWE      `json:"assigning_agency_or_department"` // CX.10
}

// PL - Person Location
type PL struct {
	PointOfCare                     string `json:"point_of_care"`                     // PL.1
	Room                            string `json:"room"`                              // PL.2
	Bed                             string `json:"bed"`                               // PL.3
	Facility                        *HD    `json:"facility"`                          // PL.4
	LocationStatus                  string `json:"location_status"`                   // PL.5
	PersonLocationType              string `json:"person_location_type"`              // PL.6
	Building                        string `json:"building"`                          // PL.7
	Floor                           string `json:"floor"`                             // PL.8
	LocationDescription             string `json:"location_description"`              // PL.9
	ComprehensiveLocationIdentifier *EI    `json:"comprehensive_location_identifier"` // PL.10
	AssigningAuthorityForLocation   *HD    `json:"assigning_authority_for_location"`  // PL.11
}

// XON - Extended Composite Name and Identification Number for Organizations
type XON struct {
	OrganizationName         string `json:"organization_name"`           // XON.1
	OrganizationNameTypeCode string `json:"organization_name_type_code"` // XON.2
	IDNumber                 string `json:"id_number"`                   // XON.3
	CheckDigit               string `json:"check_digit"`                 // XON.4
	CheckDigitScheme         string `json:"check_digit_scheme"`          // XON.5
	AssigningAuthority       *HD    `json:"assigning_authority"`         // XON.6
	IdentifierTypeCode       string `json:"identifier_type_code"`        // XON.7
	AssigningFacility        *HD    `json:"assigning_facility"`          // XON.8
	NameRepresentationCode   string `json:"name_representation_code"`    // XON.9
	OrganizationIdentifier   string `json:"organization_identifier"`     // XON.10
}

// Common segments used across message types

// MSH - Message Header Segment
type MSH struct {
	FieldSeparator                      string    `json:"field_separator"`                         // MSH.1
	EncodingCharacters                  string    `json:"encoding_characters"`                     // MSH.2
	SendingApplication                  *HD       `json:"sending_application"`                     // MSH.3
	SendingFacility                     *HD       `json:"sending_facility"`                        // MSH.4
	ReceivingApplication                *HD       `json:"receiving_application"`                   // MSH.5
	ReceivingFacility                   *HD       `json:"receiving_facility"`                      // MSH.6
	DateTimeOfMessage                   time.Time `json:"date_time_of_message"`                    // MSH.7
	Security                            string    `json:"security"`                                // MSH.8
	MessageType                         *MSG      `json:"message_type"`                            // MSH.9
	MessageControlID                    string    `json:"message_control_id"`                      // MSH.10
	ProcessingID                        *PT       `json:"processing_id"`                           // MSH.11
	VersionID                           *VID      `json:"version_id"`                              // MSH.12
	SequenceNumber                      string    `json:"sequence_number"`                         // MSH.13
	ContinuationPointer                 string    `json:"continuation_pointer"`                    // MSH.14
	AcceptAcknowledgmentType            string    `json:"accept_acknowledgment_type"`              // MSH.15
	ApplicationAcknowledgmentType       string    `json:"application_acknowledgment_type"`         // MSH.16
	CountryCode                         string    `json:"country_code"`                            // MSH.17
	CharacterSet                        []string  `json:"character_set"`                           // MSH.18
	PrincipalLanguageOfMessage          *CE       `json:"principal_language_of_message"`           // MSH.19
	AlternateCharacterSetHandlingScheme string    `json:"alternate_character_set_handling_scheme"` // MSH.20
	MessageProfileIdentifier            []*EI     `json:"message_profile_identifier"`              // MSH.21
	SendingResponsibleOrganization      *XON      `json:"sending_responsible_organization"`        // MSH.22
	ReceivingResponsibleOrganization    *XON      `json:"receiving_responsible_organization"`      // MSH.23
	SendingNetworkAddress               string    `json:"sending_network_address"`                 // MSH.24
	ReceivingNetworkAddress             string    `json:"receiving_network_address"`               // MSH.25
}

// EVN - Event Type Segment
type EVN struct {
	EventTypeCode        string    `json:"event_type_code"`         // EVN.1
	RecordedDateTime     time.Time `json:"recorded_date_time"`      // EVN.2
	DateTimePlannedEvent time.Time `json:"date_time_planned_event"` // EVN.3
	EventReasonCode      string    `json:"event_reason_code"`       // EVN.4
	OperatorID           []*XCN    `json:"operator_id"`             // EVN.5
	EventOccurred        time.Time `json:"event_occurred"`          // EVN.6
	EventFacility        *HD       `json:"event_facility"`          // EVN.7
}

// PID - Patient Identification Segment
type PID struct {
	SetID                    string    `json:"set_id"`                      // PID.1
	PatientID                *CX       `json:"patient_id"`                  // PID.2
	PatientIdentifierList    []*CX     `json:"patient_identifier_list"`     // PID.3
	AlternatePatientID       []*CX     `json:"alternate_patient_id"`        // PID.4
	PatientName              []*XPN    `json:"patient_name"`                // PID.5
	MothersMaidenName        []*XPN    `json:"mothers_maiden_name"`         // PID.6
	DateTimeOfBirth          time.Time `json:"date_time_of_birth"`          // PID.7
	AdministrativeSex        string    `json:"administrative_sex"`          // PID.8
	PatientAlias             []*XPN    `json:"patient_alias"`               // PID.9
	Race                     []*CE     `json:"race"`                        // PID.10
	PatientAddress           []*XAD    `json:"patient_address"`             // PID.11
	CountyCode               string    `json:"county_code"`                 // PID.12
	PhoneNumberHome          []*XTN    `json:"phone_number_home"`           // PID.13
	PhoneNumberBusiness      []*XTN    `json:"phone_number_business"`       // PID.14
	PrimaryLanguage          *CE       `json:"primary_language"`            // PID.15
	MaritalStatus            *CE       `json:"marital_status"`              // PID.16
	Religion                 *CE       `json:"religion"`                    // PID.17
	PatientAccountNumber     *CX       `json:"patient_account_number"`      // PID.18
	SSNNumberPatient         string    `json:"ssn_number_patient"`          // PID.19
	DriversLicenseNumber     *DLN      `json:"drivers_license_number"`      // PID.20
	MothersIdentifier        []*CX     `json:"mothers_identifier"`          // PID.21
	EthnicGroup              []*CE     `json:"ethnic_group"`                // PID.22
	BirthPlace               string    `json:"birth_place"`                 // PID.23
	MultipleBirthIndicator   string    `json:"multiple_birth_indicator"`    // PID.24
	BirthOrder               string    `json:"birth_order"`                 // PID.25
	Citizenship              []*CE     `json:"citizenship"`                 // PID.26
	VeteransMilitaryStatus   *CE       `json:"veterans_military_status"`    // PID.27
	Nationality              *CE       `json:"nationality"`                 // PID.28
	PatientDeathDateAndTime  time.Time `json:"patient_death_date_and_time"` // PID.29
	PatientDeathIndicator    string    `json:"patient_death_indicator"`     // PID.30
	IdentityUnknownIndicator string    `json:"identity_unknown_indicator"`  // PID.31
	IdentityReliabilityCode  []*CE     `json:"identity_reliability_code"`   // PID.32
	LastUpdateDateTime       time.Time `json:"last_update_date_time"`       // PID.33
	LastUpdateFacility       *HD       `json:"last_update_facility"`        // PID.34
	SpeciesCode              *CE       `json:"species_code"`                // PID.35
	BreedCode                *CE       `json:"breed_code"`                  // PID.36
	Strain                   string    `json:"strain"`                      // PID.37
	ProductionClassCode      *CE       `json:"production_class_code"`       // PID.38
	TribalCitizenship        []*CWE    `json:"tribal_citizenship"`          // PID.39
}

// DLN - Driver's License Number
type DLN struct {
	LicenseNumber  string `json:"license_number"`  // DLN.1
	IssuingState   string `json:"issuing_state"`   // DLN.2
	ExpirationDate string `json:"expiration_date"` // DLN.3
}

// NK1 - Next of Kin / Associated Parties
type NK1 struct {
	SetID                                    string    `json:"set_id"`                                         // NK1.1
	Name                                     []*XPN    `json:"name"`                                           // NK1.2
	Relationship                             *CE       `json:"relationship"`                                   // NK1.3
	Address                                  []*XAD    `json:"address"`                                        // NK1.4
	PhoneNumber                              []*XTN    `json:"phone_number"`                                   // NK1.5
	BusinessPhoneNumber                      []*XTN    `json:"business_phone_number"`                          // NK1.6
	ContactRole                              *CE       `json:"contact_role"`                                   // NK1.7
	StartDate                                time.Time `json:"start_date"`                                     // NK1.8
	EndDate                                  time.Time `json:"end_date"`                                       // NK1.9
	NextOfKinAssociatedPartiesJobTitle       string    `json:"next_of_kin_associated_parties_job_title"`       // NK1.10
	NextOfKinAssociatedPartiesJobCodeClass   *JCC      `json:"next_of_kin_associated_parties_job_code_class"`  // NK1.11
	NextOfKinAssociatedPartiesEmployeeNumber *CX       `json:"next_of_kin_associated_parties_employee_number"` // NK1.12
	OrganizationNameNK1                      []*XON    `json:"organization_name_nk1"`                          // NK1.13
	MaritalStatus                            *CE       `json:"marital_status"`                                 // NK1.14
	AdministrativeSex                        string    `json:"administrative_sex"`                             // NK1.15
	DateTimeOfBirth                          time.Time `json:"date_time_of_birth"`                             // NK1.16
	LivingDependency                         []*CE     `json:"living_dependency"`                              // NK1.17
	AmbulatoryStatus                         []*CE     `json:"ambulatory_status"`                              // NK1.18
	Citizenship                              []*CE     `json:"citizenship"`                                    // NK1.19
	PrimaryLanguage                          *CE       `json:"primary_language"`                               // NK1.20
	LivingArrangement                        string    `json:"living_arrangement"`                             // NK1.21
	PublicityCode                            *CE       `json:"publicity_code"`                                 // NK1.22
	ProtectionIndicator                      string    `json:"protection_indicator"`                           // NK1.23
	StudentIndicator                         string    `json:"student_indicator"`                              // NK1.24
	Religion                                 *CE       `json:"religion"`                                       // NK1.25
	MothersMaidenName                        []*XPN    `json:"mothers_maiden_name"`                            // NK1.26
	Nationality                              *CE       `json:"nationality"`                                    // NK1.27
	EthnicGroup                              []*CE     `json:"ethnic_group"`                                   // NK1.28
	ContactReason                            []*CE     `json:"contact_reason"`                                 // NK1.29
	ContactPersonsName                       []*XPN    `json:"contact_persons_name"`                           // NK1.30
	ContactPersonsTelephoneNumber            []*XTN    `json:"contact_persons_telephone_number"`               // NK1.31
	ContactPersonsAddress                    []*XAD    `json:"contact_persons_address"`                        // NK1.32
	NextOfKinAssociatedPartiesIdentifiers    []*CX     `json:"next_of_kin_associated_parties_identifiers"`     // NK1.33
	JobStatus                                string    `json:"job_status"`                                     // NK1.34
	Race                                     []*CE     `json:"race"`                                           // NK1.35
	Handicap                                 string    `json:"handicap"`                                       // NK1.36
	ContactPersonSocialSecurityNumber        string    `json:"contact_person_social_security_number"`          // NK1.37
	LastUpdateDateTime                       time.Time `json:"last_update_date_time"`                          // NK1.38
	LastUpdateFacility                       *HD       `json:"last_update_facility"`                           // NK1.39
	TribalCitizenship                        []*CWE    `json:"tribal_citizenship"`                             // NK1.40
	PatientSpeciesForAnimalPatients          *CE       `json:"patient_species_for_animal_patients"`            // NK1.41
	PatientBreedForAnimalPatients            *CE       `json:"patient_breed_for_animal_patients"`              // NK1.42
	BreedSubType                             string    `json:"breed_sub_type"`                                 // NK1.43
}

// JCC - Job Code Class
type JCC struct {
	JobCode        string `json:"job_code"`        // JCC.1
	JobClass       string `json:"job_class"`       // JCC.2
	JobDescription string `json:"job_description"` // JCC.3
}

// PV1 - Patient Visit Segment
type PV1 struct {
	SetID                     string      `json:"set_id"`                      // PV1.1
	PatientClass              string      `json:"patient_class"`               // PV1.2
	AssignedPatientLocation   *PL         `json:"assigned_patient_location"`   // PV1.3
	AdmissionType             string      `json:"admission_type"`              // PV1.4
	PreadmitNumber            *CX         `json:"preadmit_number"`             // PV1.5
	PriorPatientLocation      *PL         `json:"prior_patient_location"`      // PV1.6
	AttendingDoctor           []*XCN      `json:"attending_doctor"`            // PV1.7
	ReferringDoctor           []*XCN      `json:"referring_doctor"`            // PV1.8
	ConsultingDoctor          []*XCN      `json:"consulting_doctor"`           // PV1.9
	HospitalService           string      `json:"hospital_service"`            // PV1.10
	TemporaryLocation         *PL         `json:"temporary_location"`          // PV1.11
	PreadmitTestIndicator     string      `json:"preadmit_test_indicator"`     // PV1.12
	ReAdmissionIndicator      string      `json:"re_admission_indicator"`      // PV1.13
	AdmitSource               string      `json:"admit_source"`                // PV1.14
	AmbulatoryStatus          []string    `json:"ambulatory_status"`           // PV1.15
	VIPIndicator              string      `json:"vip_indicator"`               // PV1.16
	AdmittingDoctor           []*XCN      `json:"admitting_doctor"`            // PV1.17
	PatientType               string      `json:"patient_type"`                // PV1.18
	VisitNumber               *CX         `json:"visit_number"`                // PV1.19
	FinancialClass            []*FC       `json:"financial_class"`             // PV1.20
	ChargePriceIndicator      string      `json:"charge_price_indicator"`      // PV1.21
	CourtesyCode              string      `json:"courtesy_code"`               // PV1.22
	CreditRating              string      `json:"credit_rating"`               // PV1.23
	ContractCode              []string    `json:"contract_code"`               // PV1.24
	ContractEffectiveDate     []time.Time `json:"contract_effective_date"`     // PV1.25
	ContractAmount            []string    `json:"contract_amount"`             // PV1.26
	ContractPeriod            []string    `json:"contract_period"`             // PV1.27
	InterestCode              string      `json:"interest_code"`               // PV1.28
	TransferToBadDebtCode     string      `json:"transfer_to_bad_debt_code"`   // PV1.29
	TransferToBadDebtDate     time.Time   `json:"transfer_to_bad_debt_date"`   // PV1.30
	BadDebtAgencyCode         string      `json:"bad_debt_agency_code"`        // PV1.31
	BadDebtTransferAmount     string      `json:"bad_debt_transfer_amount"`    // PV1.32
	BadDebtRecoveryAmount     string      `json:"bad_debt_recovery_amount"`    // PV1.33
	DeleteAccountIndicator    string      `json:"delete_account_indicator"`    // PV1.34
	DeleteAccountDate         time.Time   `json:"delete_account_date"`         // PV1.35
	DischargeDisposition      string      `json:"discharge_disposition"`       // PV1.36
	DischargedToLocation      *DLD        `json:"discharged_to_location"`      // PV1.37
	DietType                  *CE         `json:"diet_type"`                   // PV1.38
	ServicingFacility         string      `json:"servicing_facility"`          // PV1.39
	BedStatus                 string      `json:"bed_status"`                  // PV1.40
	AccountStatus             string      `json:"account_status"`              // PV1.41
	PendingLocation           *PL         `json:"pending_location"`            // PV1.42
	PriorTemporaryLocation    *PL         `json:"prior_temporary_location"`    // PV1.43
	AdmitDateTime             time.Time   `json:"admit_date_time"`             // PV1.44
	DischargeDateTime         time.Time   `json:"discharge_date_time"`         // PV1.45
	CurrentPatientBalance     string      `json:"current_patient_balance"`     // PV1.46
	TotalCharges              string      `json:"total_charges"`               // PV1.47
	TotalAdjustments          string      `json:"total_adjustments"`           // PV1.48
	TotalPayments             string      `json:"total_payments"`              // PV1.49
	AlternateVisitID          *CX         `json:"alternate_visit_id"`          // PV1.50
	VisitIndicator            string      `json:"visit_indicator"`             // PV1.51
	OtherHealthcareProvider   []*XCN      `json:"other_healthcare_provider"`   // PV1.52
	ServiceEpisodeDescription string      `json:"service_episode_description"` // PV1.53
	ServiceEpisodeIdentifier  *CX         `json:"service_episode_identifier"`  // PV1.54
}

// FC - Financial Class
type FC struct {
	FinancialClassCode string    `json:"financial_class_code"` // FC.1
	EffectiveDate      time.Time `json:"effective_date"`       // FC.2
}

// DLD - Discharge Location and Date
type DLD struct {
	DischargeLocation string    `json:"discharge_location"` // DLD.1
	EffectiveDate     time.Time `json:"effective_date"`     // DLD.2
}

// PV2 - Patient Visit - Additional Information
type PV2 struct {
	PriorPendingLocation              *PL         `json:"prior_pending_location"`               // PV2.1
	AccommodationCode                 *CE         `json:"accommodation_code"`                   // PV2.2
	AdmitReason                       *CE         `json:"admit_reason"`                         // PV2.3
	TransferReason                    *CE         `json:"transfer_reason"`                      // PV2.4
	PatientValuables                  []string    `json:"patient_valuables"`                    // PV2.5
	PatientValuablesLocation          string      `json:"patient_valuables_location"`           // PV2.6
	VisitUserCode                     []string    `json:"visit_user_code"`                      // PV2.7
	ExpectedAdmitDateTime             time.Time   `json:"expected_admit_date_time"`             // PV2.8
	ExpectedDischargeDateTime         time.Time   `json:"expected_discharge_date_time"`         // PV2.9
	EstimatedLengthOfInpatientStay    string      `json:"estimated_length_of_inpatient_stay"`   // PV2.10
	ActualLengthOfInpatientStay       string      `json:"actual_length_of_inpatient_stay"`      // PV2.11
	VisitDescription                  string      `json:"visit_description"`                    // PV2.12
	ReferralSourceCode                []*XCN      `json:"referral_source_code"`                 // PV2.13
	PreviousServiceDate               time.Time   `json:"previous_service_date"`                // PV2.14
	EmploymentIllnessRelatedIndicator string      `json:"employment_illness_related_indicator"` // PV2.15
	PurgeStatusCode                   string      `json:"purge_status_code"`                    // PV2.16
	PurgeStatusDate                   time.Time   `json:"purge_status_date"`                    // PV2.17
	SpecialProgramCode                *CE         `json:"special_program_code"`                 // PV2.18
	RetentionIndicator                string      `json:"retention_indicator"`                  // PV2.19
	ExpectedNumberOfInsurancePlans    string      `json:"expected_number_of_insurance_plans"`   // PV2.20
	VisitPublicityCode                string      `json:"visit_publicity_code"`                 // PV2.21
	VisitProtectionIndicator          string      `json:"visit_protection_indicator"`           // PV2.22
	ClinicOrganizationName            []*XON      `json:"clinic_organization_name"`             // PV2.23
	PatientStatusCode                 string      `json:"patient_status_code"`                  // PV2.24
	VisitReasonCode                   *CE         `json:"visit_reason_code"`                    // PV2.25
	PreadmitTestIndicator             []*CE       `json:"preadmit_test_indicator"`              // PV2.26
	ReAdmissionIndicator              string      `json:"re_admission_indicator"`               // PV2.27
	AdmitSource                       *CE         `json:"admit_source"`                         // PV2.28
	AmbulatoryStatus                  []*CE       `json:"ambulatory_status"`                    // PV2.29
	VIPIndicator                      *CE         `json:"vip_indicator"`                        // PV2.30
	AdmittingDoctor                   []*XCN      `json:"admitting_doctor"`                     // PV2.31
	PatientType                       *CE         `json:"patient_type"`                         // PV2.32
	VisitNumber                       *CX         `json:"visit_number"`                         // PV2.33
	FinancialClass                    []*FC       `json:"financial_class"`                      // PV2.34
	ChargePriceIndicator              *CE         `json:"charge_price_indicator"`               // PV2.35
	CourtesyCode                      *CE         `json:"courtesy_code"`                        // PV2.36
	CreditRating                      *CE         `json:"credit_rating"`                        // PV2.37
	ContractCode                      []*CE       `json:"contract_code"`                        // PV2.38
	ContractEffectiveDate             []time.Time `json:"contract_effective_date"`              // PV2.39
	ContractAmount                    []string    `json:"contract_amount"`                      // PV2.40
	ContractPeriod                    []string    `json:"contract_period"`                      // PV2.41
	InterestCode                      *CE         `json:"interest_code"`                        // PV2.42
	TransferToBadDebtCode             *CE         `json:"transfer_to_bad_debt_code"`            // PV2.43
	TransferToBadDebtDate             time.Time   `json:"transfer_to_bad_debt_date"`            // PV2.44
	BadDebtAgencyCode                 *CE         `json:"bad_debt_agency_code"`                 // PV2.45
	BadDebtTransferAmount             string      `json:"bad_debt_transfer_amount"`             // PV2.46
	BadDebtRecoveryAmount             string      `json:"bad_debt_recovery_amount"`             // PV2.47
	DeleteAccountIndicator            *CE         `json:"delete_account_indicator"`             // PV2.48
	DeleteAccountDate                 time.Time   `json:"delete_account_date"`                  // PV2.49
	DischargeDisposition              *CE         `json:"discharge_disposition"`                // PV2.50
	DischargedToLocation              *DLD        `json:"discharged_to_location"`               // PV2.51
	DietType                          *CE         `json:"diet_type"`                            // PV2.52
	ServicingFacility                 *HD         `json:"servicing_facility"`                   // PV2.53
	BedStatus                         string      `json:"bed_status"`                           // PV2.54
	AccountStatus                     *CE         `json:"account_status"`                       // PV2.55
	PendingLocation                   *PL         `json:"pending_location"`                     // PV2.56
	PriorTemporaryLocation            *PL         `json:"prior_temporary_location"`             // PV2.57
	AdmitDateTime                     time.Time   `json:"admit_date_time"`                      // PV2.58
	DischargeDateTime                 time.Time   `json:"discharge_date_time"`                  // PV2.59
	CurrentPatientBalance             string      `json:"current_patient_balance"`              // PV2.60
	TotalCharges                      string      `json:"total_charges"`                        // PV2.61
	TotalAdjustments                  string      `json:"total_adjustments"`                    // PV2.62
	TotalPayments                     string      `json:"total_payments"`                       // PV2.63
	AlternateVisitID                  *CX         `json:"alternate_visit_id"`                   // PV2.64
	VisitIndicator                    string      `json:"visit_indicator"`                      // PV2.65
	OtherHealthcareProvider           []*XCN      `json:"other_healthcare_provider"`            // PV2.66
	ServiceEpisodeDescription         string      `json:"service_episode_description"`          // PV2.67
	ServiceEpisodeIdentifier          *CX         `json:"service_episode_identifier"`           // PV2.68
}

// AL1 - Patient Allergy Information
type AL1 struct {
	SetID                           string    `json:"set_id"`                             // AL1.1
	AllergenTypeCode                *CE       `json:"allergen_type_code"`                 // AL1.2
	AllergenCodeMnemonicDescription *CE       `json:"allergen_code_mnemonic_description"` // AL1.3
	AllergySeverityCode             *CE       `json:"allergy_severity_code"`              // AL1.4
	AllergyReactionCode             []string  `json:"allergy_reaction_code"`              // AL1.5
	IdentificationDate              time.Time `json:"identification_date"`                // AL1.6
}

// DG1 - Diagnosis
type DG1 struct {
	SetID                           string    `json:"set_id"`                             // DG1.1
	DiagnosisCodingMethod           string    `json:"diagnosis_coding_method"`            // DG1.2
	DiagnosisCodeDG1                *CE       `json:"diagnosis_code_dg1"`                 // DG1.3
	DiagnosisDescription            string    `json:"diagnosis_description"`              // DG1.4
	DiagnosisDateTime               time.Time `json:"diagnosis_date_time"`                // DG1.5
	DiagnosisType                   string    `json:"diagnosis_type"`                     // DG1.6
	MajorDiagnosticCategory         *CE       `json:"major_diagnostic_category"`          // DG1.7
	DiagnosticRelatedGroup          *CE       `json:"diagnostic_related_group"`           // DG1.8
	DRGApprovalIndicator            string    `json:"drg_approval_indicator"`             // DG1.9
	DRGGrouperReviewCode            string    `json:"drg_grouper_review_code"`            // DG1.10
	OutlierType                     *CE       `json:"outlier_type"`                       // DG1.11
	OutlierDays                     string    `json:"outlier_days"`                       // DG1.12
	OutlierCost                     string    `json:"outlier_cost"`                       // DG1.13
	GrouperVersionAndType           string    `json:"grouper_version_and_type"`           // DG1.14
	DiagnosisPriority               string    `json:"diagnosis_priority"`                 // DG1.15
	DiagnosingClinician             []*XCN    `json:"diagnosing_clinician"`               // DG1.16
	DiagnosisClassification         string    `json:"diagnosis_classification"`           // DG1.17
	ConfidentialIndicator           string    `json:"confidential_indicator"`             // DG1.18
	AttestationDateTime             time.Time `json:"attestation_date_time"`              // DG1.19
	DiagnosisIdentifier             *EI       `json:"diagnosis_identifier"`               // DG1.20
	DiagnosisActionCode             string    `json:"diagnosis_action_code"`              // DG1.21
	ParentDiagnosis                 *EI       `json:"parent_diagnosis"`                   // DG1.22
	DRGCCLValueCode                 *CE       `json:"drg_ccl_value_code"`                 // DG1.23
	DRGGroupingUsage                string    `json:"drg_grouping_usage"`                 // DG1.24
	DRGDiagnosisDeterminationStatus *CE       `json:"drg_diagnosis_determination_status"` // DG1.25
	PresentOnAdmissionIndicator     *CE       `json:"present_on_admission_indicator"`     // DG1.26
}

// OBR - Observation Request
type OBR struct {
	SetID                                      string    `json:"set_id"`                                         // OBR.1
	PlacerOrderNumber                          *EI       `json:"placer_order_number"`                            // OBR.2
	FillerOrderNumber                          *EI       `json:"filler_order_number"`                            // OBR.3
	UniversalServiceIdentifier                 *CE       `json:"universal_service_identifier"`                   // OBR.4
	PriorityOBR                                string    `json:"priority_obr"`                                   // OBR.5
	RequestedDateTime                          time.Time `json:"requested_date_time"`                            // OBR.6
	ObservationDateTime                        time.Time `json:"observation_date_time"`                          // OBR.7
	ObservationEndDateTime                     time.Time `json:"observation_end_date_time"`                      // OBR.8
	CollectionVolume                           *CQ       `json:"collection_volume"`                              // OBR.9
	CollectorIdentifier                        []*XCN    `json:"collector_identifier"`                           // OBR.10
	SpecimenActionCode                         string    `json:"specimen_action_code"`                           // OBR.11
	DangerCode                                 *CE       `json:"danger_code"`                                    // OBR.12
	RelevantClinicalInfo                       string    `json:"relevant_clinical_info"`                         // OBR.13
	SpecimenReceivedDateTime                   time.Time `json:"specimen_received_date_time"`                    // OBR.14
	SpecimenSource                             *SPS      `json:"specimen_source"`                                // OBR.15
	OrderingProvider                           []*XCN    `json:"ordering_provider"`                              // OBR.16
	OrderCallbackPhoneNumber                   []*XTN    `json:"order_callback_phone_number"`                    // OBR.17
	PlacerField1                               string    `json:"placer_field1"`                                  // OBR.18
	PlacerField2                               string    `json:"placer_field2"`                                  // OBR.19
	FillerField1                               string    `json:"filler_field1"`                                  // OBR.20
	FillerField2                               string    `json:"filler_field2"`                                  // OBR.21
	ResultsRptStatusChngDateTime               time.Time `json:"results_rpt_status_chng_date_time"`              // OBR.22
	ChargeToPractice                           *MOC      `json:"charge_to_practice"`                             // OBR.23
	DiagnosticServSectID                       string    `json:"diagnostic_serv_sect_id"`                        // OBR.24
	ResultStatus                               string    `json:"result_status"`                                  // OBR.25
	ParentResult                               *PRL      `json:"parent_result"`                                  // OBR.26
	QuantityTiming                             []string  `json:"quantity_timing"`                                // OBR.27
	ResultCopiesTo                             []*XCN    `json:"result_copies_to"`                               // OBR.28
	ParentNumber                               *EIP      `json:"parent_number"`                                  // OBR.29
	TransportationMode                         string    `json:"transportation_mode"`                            // OBR.30
	ReasonForStudy                             []*CE     `json:"reason_for_study"`                               // OBR.31
	PrincipalResultInterpreter                 *NDL      `json:"principal_result_interpreter"`                   // OBR.32
	AssistantResultInterpreter                 []*NDL    `json:"assistant_result_interpreter"`                   // OBR.33
	Technician                                 []*NDL    `json:"technician"`                                     // OBR.34
	Transcriptionist                           []*NDL    `json:"transcriptionist"`                               // OBR.35
	ScheduledDateTime                          time.Time `json:"scheduled_date_time"`                            // OBR.36
	NumberOfSampleContainers                   string    `json:"number_of_sample_containers"`                    // OBR.37
	TransportLogisticsOfCollectedSample        []string  `json:"transport_logistics_of_collected_sample"`        // OBR.38
	CollectorsComment                          []*CE     `json:"collectors_comment"`                             // OBR.39
	TransportArrangementResponsibility         string    `json:"transport_arrangement_responsibility"`           // OBR.40
	TransportArranged                          string    `json:"transport_arranged"`                             // OBR.41
	EscortRequired                             string    `json:"escort_required"`                                // OBR.42
	PlannedPatientTransportComment             []*CE     `json:"planned_patient_transport_comment"`              // OBR.43
	ProcedureCode                              *CE       `json:"procedure_code"`                                 // OBR.44
	ProcedureCodeModifier                      []*CE     `json:"procedure_code_modifier"`                        // OBR.45
	PlacerSupplementalServiceInformation       []*CE     `json:"placer_supplemental_service_information"`        // OBR.46
	FillerSupplementalServiceInformation       []*CE     `json:"filler_supplemental_service_information"`        // OBR.47
	MedicallyNecessaryDuplicateProcedureReason *CWE      `json:"medically_necessary_duplicate_procedure_reason"` // OBR.48
	ResultHandling                             string    `json:"result_handling"`                                // OBR.49
	ParentUniversalServiceIdentifier           *CWE      `json:"parent_universal_service_identifier"`            // OBR.50
}

// CQ - Composite Quantity with Units
type CQ struct {
	Quantity string `json:"quantity"` // CQ.1
	Units    *CE    `json:"units"`    // CQ.2
}

// SPS - Specimen Source
type SPS struct {
	SpecimenSourceNameOrCode     *CE    `json:"specimen_source_name_or_code"`    // SPS.1
	Additives                    string `json:"additives"`                       // SPS.2
	SpecimenCollectionMethod     string `json:"specimen_collection_method"`      // SPS.3
	BodySite                     *CE    `json:"body_site"`                       // SPS.4
	SiteModifier                 *CE    `json:"site_modifier"`                   // SPS.5
	CollectionMethodModifierCode *CE    `json:"collection_method_modifier_code"` // SPS.6
	SpecimenRole                 *CWE   `json:"specimen_role"`                   // SPS.7
}

// MOC - Money and Charge Code
type MOC struct {
	MonetaryAmount string `json:"monetary_amount"` // MOC.1
	ChargeCode     *CE    `json:"charge_code"`     // MOC.2
}

// PRL - Parent Result Link
type PRL struct {
	ParentObservationIdentifier      *CE    `json:"parent_observation_identifier"`       // PRL.1
	ParentObservationSubIdentifier   string `json:"parent_observation_sub_identifier"`   // PRL.2
	ParentObservationValueDescriptor string `json:"parent_observation_value_descriptor"` // PRL.3
}

// EIP - Entity Identifier Pair
type EIP struct {
	PlacerAssignedIdentifier *EI `json:"placer_assigned_identifier"` // EIP.1
	FillerAssignedIdentifier *EI `json:"filler_assigned_identifier"` // EIP.2
}

// NDL - Name with Date and Location
type NDL struct {
	Name                *XCN      `json:"name"`                  // NDL.1
	StartDateTime       time.Time `json:"start_date_time"`       // NDL.2
	EndDateTime         time.Time `json:"end_date_time"`         // NDL.3
	PointOfCare         string    `json:"point_of_care"`         // NDL.4
	Room                string    `json:"room"`                  // NDL.5
	Bed                 string    `json:"bed"`                   // NDL.6
	Facility            *HD       `json:"facility"`              // NDL.7
	LocationStatus      string    `json:"location_status"`       // NDL.8
	PatientLocationType string    `json:"patient_location_type"` // NDL.9
	Building            string    `json:"building"`              // NDL.10
	Floor               string    `json:"floor"`                 // NDL.11
}

// OBX - Observation/Result
type OBX struct {
	SetID                                 string      `json:"set_id"`                                   // OBX.1
	ValueType                             string      `json:"value_type"`                               // OBX.2
	ObservationIdentifier                 *CE         `json:"observation_identifier"`                   // OBX.3
	ObservationSubID                      string      `json:"observation_sub_id"`                       // OBX.4
	ObservationValue                      interface{} `json:"observation_value"`                        // OBX.5
	Units                                 *CE         `json:"units"`                                    // OBX.6
	ReferencesRange                       string      `json:"references_range"`                         // OBX.7
	AbnormalFlags                         []string    `json:"abnormal_flags"`                           // OBX.8
	Probability                           string      `json:"probability"`                              // OBX.9
	NatureOfAbnormalTest                  []string    `json:"nature_of_abnormal_test"`                  // OBX.10
	ObservationResultStatus               string      `json:"observation_result_status"`                // OBX.11
	EffectiveDateOfReferenceRange         time.Time   `json:"effective_date_of_reference_range"`        // OBX.12
	UserDefinedAccessChecks               string      `json:"user_defined_access_checks"`               // OBX.13
	DateTimeOfTheObservation              time.Time   `json:"date_time_of_the_observation"`             // OBX.14
	ProducersID                           *CE         `json:"producers_id"`                             // OBX.15
	ResponsibleObserver                   []*XCN      `json:"responsible_observer"`                     // OBX.16
	ObservationMethod                     []*CE       `json:"observation_method"`                       // OBX.17
	EquipmentInstanceIdentifier           *EI         `json:"equipment_instance_identifier"`            // OBX.18
	DateTimeOfTheAnalysis                 time.Time   `json:"date_time_of_the_analysis"`                // OBX.19
	ObservationSite                       []*CE       `json:"observation_site"`                         // OBX.20
	ObservationInstanceIdentifier         *EI         `json:"observation_instance_identifier"`          // OBX.21
	MoodCode                              string      `json:"mood_code"`                                // OBX.22
	PerformingOrganizationName            *XON        `json:"performing_organization_name"`             // OBX.23
	PerformingOrganizationAddress         *XAD        `json:"performing_organization_address"`          // OBX.24
	PerformingOrganizationMedicalDirector []*XCN      `json:"performing_organization_medical_director"` // OBX.25
	PatientResultsReleaseCategory         string      `json:"patient_results_release_category"`         // OBX.26
	RootCause                             string      `json:"root_cause"`                               // OBX.27
	LocalProcessControl                   []*CE       `json:"local_process_control"`                    // OBX.28
}

// ORC - Common Order
type ORC struct {
	OrderControl                            string    `json:"order_control"`                               // ORC.1
	PlacerOrderNumber                       *EI       `json:"placer_order_number"`                         // ORC.2
	FillerOrderNumber                       *EI       `json:"filler_order_number"`                         // ORC.3
	PlacerGroupNumber                       *EI       `json:"placer_group_number"`                         // ORC.4
	OrderStatus                             string    `json:"order_status"`                                // ORC.5
	ResponseFlag                            string    `json:"response_flag"`                               // ORC.6
	QuantityTiming                          []string  `json:"quantity_timing"`                             // ORC.7
	ParentOrder                             *EIP      `json:"parent_order"`                                // ORC.8
	DateTimeOfTransaction                   time.Time `json:"date_time_of_transaction"`                    // ORC.9
	EnteredBy                               []*XCN    `json:"entered_by"`                                  // ORC.10
	VerifiedBy                              []*XCN    `json:"verified_by"`                                 // ORC.11
	OrderingProvider                        []*XCN    `json:"ordering_provider"`                           // ORC.12
	EnterersLocation                        *PL       `json:"enterers_location"`                           // ORC.13
	CallBackPhoneNumber                     []*XTN    `json:"call_back_phone_number"`                      // ORC.14
	OrderEffectiveDateTime                  time.Time `json:"order_effective_date_time"`                   // ORC.15
	OrderControlCodeReason                  *CE       `json:"order_control_code_reason"`                   // ORC.16
	EnteringOrganization                    *CE       `json:"entering_organization"`                       // ORC.17
	EnteringDevice                          *CE       `json:"entering_device"`                             // ORC.18
	ActionBy                                []*XCN    `json:"action_by"`                                   // ORC.19
	AdvancedBeneficiaryNoticeCode           *CE       `json:"advanced_beneficiary_notice_code"`            // ORC.20
	OrderingFacilityName                    []*XON    `json:"ordering_facility_name"`                      // ORC.21
	OrderingFacilityAddress                 []*XAD    `json:"ordering_facility_address"`                   // ORC.22
	OrderingFacilityPhoneNumber             []*XTN    `json:"ordering_facility_phone_number"`              // ORC.23
	OrderingProviderAddress                 []*XAD    `json:"ordering_provider_address"`                   // ORC.24
	OrderStatusModifier                     *CWE      `json:"order_status_modifier"`                       // ORC.25
	AdvancedBeneficiaryNoticeOverrideReason *CWE      `json:"advanced_beneficiary_notice_override_reason"` // ORC.26
	FillersExpectedAvailabilityDateTime     time.Time `json:"fillers_expected_availability_date_time"`     // ORC.27
	ConfidentialityCode                     *CWE      `json:"confidentiality_code"`                        // ORC.28
	OrderType                               *CWE      `json:"order_type"`                                  // ORC.29
	EntererAuthorizationCode                string    `json:"enterer_authorization_code"`                  // ORC.30
}

// ROL - Role
type ROL struct {
	RoleInstanceID              string    `json:"role_instance_id"`               // ROL.1
	ActionCode                  string    `json:"action_code"`                    // ROL.2
	RoleROL                     *CE       `json:"role_rol"`                       // ROL.3
	RolePerson                  []*XCN    `json:"role_person"`                    // ROL.4
	RoleBeginDateTime           time.Time `json:"role_begin_date_time"`           // ROL.5
	RoleEndDateTime             time.Time `json:"role_end_date_time"`             // ROL.6
	RoleDuration                *CE       `json:"role_duration"`                  // ROL.7
	RoleActionReason            *CE       `json:"role_action_reason"`             // ROL.8
	ProviderType                []*CE     `json:"provider_type"`                  // ROL.9
	OrganizationUnitType        *CE       `json:"organization_unit_type"`         // ROL.10
	OfficeHomeAddressBirthplace *XAD      `json:"office_home_address_birthplace"` // ROL.11
	Phone                       []*XTN    `json:"phone"`                          // ROL.12
}

// TXA - Transcription Document Header
type TXA struct {
	SetID                           string      `json:"set_id"`                              // TXA.1
	DocumentType                    string      `json:"document_type"`                       // TXA.2
	DocumentContentPresentation     string      `json:"document_content_presentation"`       // TXA.3
	ActivityDateTime                time.Time   `json:"activity_date_time"`                  // TXA.4
	PrimaryActivityProviderCodeName []*XCN      `json:"primary_activity_provider_code_name"` // TXA.5
	OriginationDateTime             time.Time   `json:"origination_date_time"`               // TXA.6
	TranscriptionDateTime           time.Time   `json:"transcription_date_time"`             // TXA.7
	EditDateTime                    []time.Time `json:"edit_date_time"`                      // TXA.8
	OriginatorCodeName              []*XCN      `json:"originator_code_name"`                // TXA.9
	AssignedDocumentAuthenticator   []*XCN      `json:"assigned_document_authenticator"`     // TXA.10
	TranscriptionistCodeName        []*XCN      `json:"transcriptionist_code_name"`          // TXA.11
	UniqueDocumentNumber            string      `json:"unique_document_number"`              // TXA.12
	ParentDocumentNumber            string      `json:"parent_document_number"`              // TXA.13
	PlacerOrderNumber               []string    `json:"placer_order_number"`                 // TXA.14
	FillerOrderNumber               string      `json:"filler_order_number"`                 // TXA.15
	UniqueDocumentFileName          string      `json:"unique_document_file_name"`           // TXA.16
	DocumentCompletionStatus        string      `json:"document_completion_status"`          // TXA.17
	DocumentConfidentialityStatus   string      `json:"document_confidentiality_status"`     // TXA.18
	DocumentAvailabilityStatus      string      `json:"document_availability_status"`        // TXA.19
	DocumentStorageStatus           string      `json:"document_storage_status"`             // TXA.20
	DocumentChangeReason            string      `json:"document_change_reason"`              // TXA.21
	AuthenticationPersonTimeStamp   []*XCN      `json:"authentication_person_time_stamp"`    // TXA.22
	DistributedCopiesCodeandName    []*XCN      `json:"distributed_copies_codeand_name"`     // TXA.23
}

// SCH - Scheduling Activity Information
type SCH struct {
	PlacerAppointmentID                  string   `json:"placer_appointment_id"`                    // SCH.1
	FillerAppointmentID                  string   `json:"filler_appointment_id"`                    // SCH.2
	OccurrenceNumber                     string   `json:"occurrence_number"`                        // SCH.3
	PlacerGroupNumber                    string   `json:"placer_group_number"`                      // SCH.4
	ScheduleID                           string   `json:"schedule_id"`                              // SCH.5
	EventReason                          *CE      `json:"event_reason"`                             // SCH.6
	AppointmentReason                    *CE      `json:"appointment_reason"`                       // SCH.7
	AppointmentType                      *CE      `json:"appointment_type"`                         // SCH.8
	AppointmentDuration                  string   `json:"appointment_duration"`                     // SCH.9
	AppointmentDurationUnits             *CE      `json:"appointment_duration_units"`               // SCH.10
	AppointmentTimingQuantity            []string `json:"appointment_timing_quantity"`              // SCH.11
	PlacerContactPerson                  []*XCN   `json:"placer_contact_person"`                    // SCH.12
	PlacerContactPhoneNumber             []*XTN   `json:"placer_contact_phone_number"`              // SCH.13
	PlacerContactAddress                 []*XAD   `json:"placer_contact_address"`                   // SCH.14
	PlacerContactLocation                *PL      `json:"placer_contact_location"`                  // SCH.15
	FillerContactPerson                  []*XCN   `json:"filler_contact_person"`                    // SCH.16
	FillerContactPhoneNumber             []*XTN   `json:"filler_contact_phone_number"`              // SCH.17
	FillerContactAddress                 []*XAD   `json:"filler_contact_address"`                   // SCH.18
	FillerContactLocation                *PL      `json:"filler_contact_location"`                  // SCH.19
	EnteredByPerson                      []*XCN   `json:"entered_by_person"`                        // SCH.20
	EnteredByPhoneNumber                 []*XTN   `json:"entered_by_phone_number"`                  // SCH.21
	EnteredByLocation                    *PL      `json:"entered_by_location"`                      // SCH.22
	ParentPlacerAppointmentID            string   `json:"parent_placer_appointment_id"`             // SCH.23
	ParentFillerAppointmentID            string   `json:"parent_filler_appointment_id"`             // SCH.24
	FillerStatusCode                     *CE      `json:"filler_status_code"`                       // SCH.25
	PlacerStatusCode                     *CE      `json:"placer_status_code"`                       // SCH.26
	FillerSupplementalServiceInformation []*CE    `json:"filler_supplemental_service_information"`  // SCH.27
	PlacerSupplementalServiceInformation []*CE    `json:"placer_supplemental_service_information"`  // SCH.28
	RequestedNewAppointmentBookingHandle string   `json:"requested_new_appointment_booking_handle"` // SCH.29
	ReferencedBy                         string   `json:"referenced_by"`                            // SCH.30
}

// RGS - Resource Group
type RGS struct {
	SetID                                  string   `json:"set_id"`                                      // RGS.1
	SegmentActionCode                      string   `json:"segment_action_code"`                         // RGS.2
	ResourceGroupID                        string   `json:"resource_group_id"`                           // RGS.3
	ResourceGroupName                      string   `json:"resource_group_name"`                         // RGS.4
	CoordinatedResourceGroupScheduleIDList []string `json:"coordinated_resource_group_schedule_id_list"` // RGS.5
}

// AIG - Appointment Information - General Resource
type AIG struct {
	SetID                    string    `json:"set_id"`                       // AIG.1
	SegmentActionCode        string    `json:"segment_action_code"`          // AIG.2
	ResourceID               *CE       `json:"resource_id"`                  // AIG.3
	ResourceType             *CE       `json:"resource_type"`                // AIG.4
	ResourceGroup            []*CE     `json:"resource_group"`               // AIG.5
	ResourceQuantity         string    `json:"resource_quantity"`            // AIG.6
	ResourceQuantityUnits    *CE       `json:"resource_quantity_units"`      // AIG.7
	StartDateTime            time.Time `json:"start_date_time"`              // AIG.8
	StartDateTimeOffset      string    `json:"start_date_time_offset"`       // AIG.9
	StartDateTimeOffsetUnits *CE       `json:"start_date_time_offset_units"` // AIG.10
	Duration                 string    `json:"duration"`                     // AIG.11
	DurationUnits            *CE       `json:"duration_units"`               // AIG.12
	AllowSubstitutionCode    string    `json:"allow_substitution_code"`      // AIG.13
	FillerStatusCode         *CE       `json:"filler_status_code"`           // AIG.14
}

// AIL - Appointment Information - Location Resource
type AIL struct {
	SetID                    string    `json:"set_id"`                       // AIL.1
	SegmentActionCode        string    `json:"segment_action_code"`          // AIL.2
	LocationResourceID       []*PL     `json:"location_resource_id"`         // AIL.3
	LocationTypeAIL          *CE       `json:"location_type_ail"`            // AIL.4
	LocationGroup            *CE       `json:"location_group"`               // AIL.5
	StartDateTime            time.Time `json:"start_date_time"`              // AIL.6
	StartDateTimeOffset      string    `json:"start_date_time_offset"`       // AIL.7
	StartDateTimeOffsetUnits *CE       `json:"start_date_time_offset_units"` // AIL.8
	Duration                 string    `json:"duration"`                     // AIL.9
	DurationUnits            *CE       `json:"duration_units"`               // AIL.10
	AllowSubstitutionCode    string    `json:"allow_substitution_code"`      // AIL.11
	FillerStatusCode         *CE       `json:"filler_status_code"`           // AIL.12
}

// AIS - Appointment Information - Service
type AIS struct {
	SetID                      string    `json:"set_id"`                       // AIS.1
	SegmentActionCode          string    `json:"segment_action_code"`          // AIS.2
	UniversalServiceIdentifier *CE       `json:"universal_service_identifier"` // AIS.3
	StartDateTime              time.Time `json:"start_date_time"`              // AIS.4
	StartDateTimeOffset        string    `json:"start_date_time_offset"`       // AIS.5
	StartDateTimeOffsetUnits   *CE       `json:"start_date_time_offset_units"` // AIS.6
	Duration                   string    `json:"duration"`                     // AIS.7
	DurationUnits              *CE       `json:"duration_units"`               // AIS.8
	AllowSubstitutionCode      string    `json:"allow_substitution_code"`      // AIS.9
	FillerStatusCode           *CE       `json:"filler_status_code"`           // AIS.10
}

// MSA - Message Acknowledgment
type MSA struct {
	AcknowledgmentCode        string `json:"acknowledgment_code"`         // MSA.1
	MessageControlID          string `json:"message_control_id"`          // MSA.2
	TextMessage               string `json:"text_message"`                // MSA.3
	ExpectedSequenceNumber    string `json:"expected_sequence_number"`    // MSA.4
	DelayedAcknowledgmentType string `json:"delayed_acknowledgment_type"` // MSA.5
	ErrorCondition            *CE    `json:"error_condition"`             // MSA.6
}

// ERR - Error
type ERR struct {
	ErrorCodeAndLocation      []*ELD   `json:"error_code_and_location"`     // ERR.1
	ErrorLocation             []*ERL   `json:"error_location"`              // ERR.2
	HL7ErrorCode              *CE      `json:"hl7_error_code"`              // ERR.3
	Severity                  string   `json:"severity"`                    // ERR.4
	ApplicationErrorCode      *CE      `json:"application_error_code"`      // ERR.5
	ApplicationErrorParameter []string `json:"application_error_parameter"` // ERR.6
	DiagnosticInformation     string   `json:"diagnostic_information"`      // ERR.7
	UserMessage               string   `json:"user_message"`                // ERR.8
	InformPersonIndicator     []string `json:"inform_person_indicator"`     // ERR.9
	OverrideType              *CE      `json:"override_type"`               // ERR.10
	OverrideReasonCode        []*CE    `json:"override_reason_code"`        // ERR.11
	HelpDeskContactPoint      []*XTN   `json:"help_desk_contact_point"`     // ERR.12
}

// ELD - Error Location and Description
type ELD struct {
	SegmentID            string `json:"segment_id"`             // ELD.1
	SegmentSequence      string `json:"segment_sequence"`       // ELD.2
	FieldPosition        string `json:"field_position"`         // ELD.3
	CodeIdentifyingError *CE    `json:"code_identifying_error"` // ELD.4
}

// ERL - Error Location
type ERL struct {
	SegmentID          string `json:"segment_id"`           // ERL.1
	SegmentSequence    string `json:"segment_sequence"`     // ERL.2
	FieldPosition      string `json:"field_position"`       // ERL.3
	FieldRepetition    string `json:"field_repetition"`     // ERL.4
	ComponentNumber    string `json:"component_number"`     // ERL.5
	SubComponentNumber string `json:"sub_component_number"` // ERL.6
}

// FT1 - Financial Transaction
type FT1 struct {
	SetID                     string    `json:"set_id"`                      // FT1.1
	TransactionID             string    `json:"transaction_id"`              // FT1.2
	TransactionBatchID        string    `json:"transaction_batch_id"`        // FT1.3
	TransactionDate           time.Time `json:"transaction_date"`            // FT1.4
	TransactionPostingDate    time.Time `json:"transaction_posting_date"`    // FT1.5
	TransactionType           string    `json:"transaction_type"`            // FT1.6
	TransactionCode           *CE       `json:"transaction_code"`            // FT1.7
	TransactionDescription    string    `json:"transaction_description"`     // FT1.8
	TransactionDescriptionAlt string    `json:"transaction_description_alt"` // FT1.9
	TransactionQuantity       string    `json:"transaction_quantity"`        // FT1.10
	TransactionAmountExtended string    `json:"transaction_amount_extended"` // FT1.11
	TransactionAmountUnit     string    `json:"transaction_amount_unit"`     // FT1.12
	DepartmentCode            *CE       `json:"department_code"`             // FT1.13
	InsurancePlanID           *CE       `json:"insurance_plan_id"`           // FT1.14
	InsuranceAmount           string    `json:"insurance_amount"`            // FT1.15
	AssignedPatientLocation   *PL       `json:"assigned_patient_location"`   // FT1.16
	FeeSchedule               string    `json:"fee_schedule"`                // FT1.17
	PatientType               string    `json:"patient_type"`                // FT1.18
	DiagnosisCodeFT1          []*CE     `json:"diagnosis_code_ft1"`          // FT1.19
	PerformedByCode           []*XCN    `json:"performed_by_code"`           // FT1.20
	OrderedByCode             []*XCN    `json:"ordered_by_code"`             // FT1.21
	UnitCost                  string    `json:"unit_cost"`                   // FT1.22
	FillerOrderNumber         string    `json:"filler_order_number"`         // FT1.23
	EnteredByCode             []*XCN    `json:"entered_by_code"`             // FT1.24
	ProcedureCode             *CE       `json:"procedure_code"`              // FT1.25
	ProcedureCodeModifier     []*CE     `json:"procedure_code_modifier"`     // FT1.26
}

// PR1 - Procedures
type PR1 struct {
	SetID                   string    `json:"set_id"`                    // PR1.1
	ProcedureCodingMethod   string    `json:"procedure_coding_method"`   // PR1.2
	ProcedureCode           *CE       `json:"procedure_code"`            // PR1.3
	ProcedureDescription    string    `json:"procedure_description"`     // PR1.4
	ProcedureDateTime       time.Time `json:"procedure_date_time"`       // PR1.5
	ProcedureFunctionalType string    `json:"procedure_functional_type"` // PR1.6
	ProcedureMinutes        string    `json:"procedure_minutes"`         // PR1.7
	Anesthesiologist        []*XCN    `json:"anesthesiologist"`          // PR1.8
	AnesthesiaCode          string    `json:"anesthesia_code"`           // PR1.9
	AnesthesiaMinutes       string    `json:"anesthesia_minutes"`        // PR1.10
	Surgeon                 []*XCN    `json:"surgeon"`                   // PR1.11
	ProcedurePractitioner   []*XCN    `json:"procedure_practitioner"`    // PR1.12
	ConsentCode             *CE       `json:"consent_code"`              // PR1.13
	ProcedurePriority       string    `json:"procedure_priority"`        // PR1.14
	AssociatedDiagnosisCode *CE       `json:"associated_diagnosis_code"` // PR1.15
	ProcedureCodeModifier   []*CE     `json:"procedure_code_modifier"`   // PR1.16
	ProcedureDRGType        string    `json:"procedure_drg_type"`        // PR1.17
	TissueTypeCode          []*CE     `json:"tissue_type_code"`          // PR1.18
}

// SPM - Specimen
type SPM struct {
	SetID                      string    `json:"set_id"`                        // SPM.1
	SpecimenID                 string    `json:"specimen_id"`                   // SPM.2
	SpecimenParentIDs          []string  `json:"specimen_parent_i_ds"`          // SPM.3
	SpecimenType               *CE       `json:"specimen_type"`                 // SPM.4
	SpecimenTypeModifier       []*CE     `json:"specimen_type_modifier"`        // SPM.5
	SpecimenAdditives          []*CE     `json:"specimen_additives"`            // SPM.6
	SpecimenCollectionMethod   *CE       `json:"specimen_collection_method"`    // SPM.7
	SpecimenSourceSite         *CE       `json:"specimen_source_site"`          // SPM.8
	SpecimenSourceSiteModifier []*CE     `json:"specimen_source_site_modifier"` // SPM.9
	SpecimenCollectionSite     *CE       `json:"specimen_collection_site"`      // SPM.10
	SpecimenRole               []*CE     `json:"specimen_role"`                 // SPM.11
	SpecimenCollectionAmount   string    `json:"specimen_collection_amount"`    // SPM.12
	GroupedSpecimenCount       string    `json:"grouped_specimen_count"`        // SPM.13
	SpecimenDescription        []string  `json:"specimen_description"`          // SPM.14
	SpecimenHandlingCode       []*CE     `json:"specimen_handling_code"`        // SPM.15
	SpecimenRiskCode           []*CE     `json:"specimen_risk_code"`            // SPM.16
	SpecimenCollectionDateTime time.Time `json:"specimen_collection_date_time"` // SPM.17
	SpecimenReceivedDateTime   time.Time `json:"specimen_received_date_time"`   // SPM.18
	SpecimenExpirationDateTime time.Time `json:"specimen_expiration_date_time"` // SPM.19
	SpecimenAvailability       string    `json:"specimen_availability"`         // SPM.20
	SpecimenRejectReason       []*CE     `json:"specimen_reject_reason"`        // SPM.21
	SpecimenQuality            *CE       `json:"specimen_quality"`              // SPM.22
	SpecimenAppropriateness    *CE       `json:"specimen_appropriateness"`      // SPM.23
	SpecimenCondition          []*CE     `json:"specimen_condition"`            // SPM.24
	SpecimenCurrentQuantity    string    `json:"specimen_current_quantity"`     // SPM.25
	NumberOfSpecimenContainers string    `json:"number_of_specimen_containers"` // SPM.26
	ContainerType              *CE       `json:"container_type"`                // SPM.27
	ContainerCondition         *CE       `json:"container_condition"`           // SPM.28
	SpecimenChildRole          *CE       `json:"specimen_child_role"`           // SPM.29
}

// SAC - Specimen and Container Detail
type SAC struct {
	ExternalAccessionIdentifier  string    `json:"external_accession_identifier"`   // SAC.1
	AccessionIdentifier          string    `json:"accession_identifier"`            // SAC.2
	ContainerIdentifier          string    `json:"container_identifier"`            // SAC.3
	PrimaryParentContainerID     string    `json:"primary_parent_container_id"`     // SAC.4
	EquipmentContainerID         string    `json:"equipment_container_id"`          // SAC.5
	SpecimenSource               string    `json:"specimen_source"`                 // SAC.6
	RegistrationDateTime         time.Time `json:"registration_date_time"`          // SAC.7
	ContainerStatus              *CE       `json:"container_status"`                // SAC.8
	CarrierType                  *CE       `json:"carrier_type"`                    // SAC.9
	CarrierIdentifier            string    `json:"carrier_identifier"`              // SAC.10
	PositionInCarrier            string    `json:"position_in_carrier"`             // SAC.11
	TrayTypeSAC                  *CE       `json:"tray_type_sac"`                   // SAC.12
	TrayIdentifier               string    `json:"tray_identifier"`                 // SAC.13
	PositionInTray               string    `json:"position_in_tray"`                // SAC.14
	Location                     []*CE     `json:"location"`                        // SAC.15
	ContainerHeight              string    `json:"container_height"`                // SAC.16
	ContainerDiameter            string    `json:"container_diameter"`              // SAC.17
	BarrierDelta                 string    `json:"barrier_delta"`                   // SAC.18
	BottomDelta                  string    `json:"bottom_delta"`                    // SAC.19
	ContainerHeightDiameterUnits string    `json:"container_height_diameter_units"` // SAC.20
	ContainerVolume              string    `json:"container_volume"`                // SAC.21
	AvailableSpecimenVolume      string    `json:"available_specimen_volume"`       // SAC.22
	InitialSpecimenVolume        string    `json:"initial_specimen_volume"`         // SAC.23
	VolumeUnits                  *CE       `json:"volume_units"`                    // SAC.24
	SeparatorType                *CE       `json:"separator_type"`                  // SAC.25
	CapType                      *CE       `json:"cap_type"`                        // SAC.26
	Additive                     []*CE     `json:"additive"`                        // SAC.27
	SpecimenComponent            *CE       `json:"specimen_component"`              // SAC.28
	DilutionFactor               string    `json:"dilution_factor"`                 // SAC.29
	Treatment                    *CE       `json:"treatment"`                       // SAC.30
	Temperature                  string    `json:"temperature"`                     // SAC.31
	HemolysisIndex               string    `json:"hemolysis_index"`                 // SAC.32
	HemolysisIndexUnits          *CE       `json:"hemolysis_index_units"`           // SAC.33
	LipemiaIndex                 string    `json:"lipemia_index"`                   // SAC.34
	LipemiaIndexUnits            *CE       `json:"lipemia_index_units"`             // SAC.35
	IcterusIndex                 string    `json:"icterus_index"`                   // SAC.36
	IcterusIndexUnits            *CE       `json:"icterus_index_units"`             // SAC.37
	FibrinIndex                  string    `json:"fibrin_index"`                    // SAC.38
	FibrinIndexUnits             *CE       `json:"fibrin_index_units"`              // SAC.39
	SystemInducedContaminants    []*CE     `json:"system_induced_contaminants"`     // SAC.40
	DrugInterference             []*CE     `json:"drug_interference"`               // SAC.41
	ArtificialBlood              *CE       `json:"artificial_blood"`                // SAC.42
	SpecialHandlingCode          []*CE     `json:"special_handling_code"`           // SAC.43
	OtherEnvironmentalFactors    []*CE     `json:"other_environmental_factors"`     // SAC.44
}

// RXE - Pharmacy/Treatment Encoded Order
type RXE struct {
	QuantityTiming                                          string    `json:"quantity_timing"`                                              // RXE.1
	GiveCode                                                *CE       `json:"give_code"`                                                    // RXE.2
	GiveAmountMinimum                                       string    `json:"give_amount_minimum"`                                          // RXE.3
	GiveAmountMaximum                                       string    `json:"give_amount_maximum"`                                          // RXE.4
	GiveUnits                                               *CE       `json:"give_units"`                                                   // RXE.5
	GiveDosageForm                                          *CE       `json:"give_dosage_form"`                                             // RXE.6
	ProvidersAdministrationInstructions                     []*CE     `json:"providers_administration_instructions"`                        // RXE.7
	DeliverToLocation                                       *PL       `json:"deliver_to_location"`                                          // RXE.8
	SubstitutionStatus                                      string    `json:"substitution_status"`                                          // RXE.9
	DispenseAmount                                          string    `json:"dispense_amount"`                                              // RXE.10
	DispenseUnits                                           *CE       `json:"dispense_units"`                                               // RXE.11
	NumberOfRefills                                         string    `json:"number_of_refills"`                                            // RXE.12
	OrderingProvidersOrderNumber                            []*XCN    `json:"ordering_providers_order_number"`                              // RXE.13
	NumberOfRefillsRemaining                                string    `json:"number_of_refills_remaining"`                                  // RXE.14
	NumberOfRefillsDosesDispensed                           string    `json:"number_of_refills_doses_dispensed"`                            // RXE.15
	DateTimeOfMostRecentRefillOrDoseDispensed               time.Time `json:"date_time_of_most_recent_refill_or_dose_dispensed"`            // RXE.16
	TotalDailyDose                                          string    `json:"total_daily_dose"`                                             // RXE.17
	NeedsHumanReview                                        string    `json:"needs_human_review"`                                           // RXE.18
	PharmacyTreatmentSuppliersSpecialDispensingInstructions []*CE     `json:"pharmacy_treatment_suppliers_special_dispensing_instructions"` // RXE.19
	GivePerTimeUnit                                         string    `json:"give_per_time_unit"`                                           // RXE.20
	GiveRateAmount                                          string    `json:"give_rate_amount"`                                             // RXE.21
	GiveRateUnits                                           *CE       `json:"give_rate_units"`                                              // RXE.22
	GiveStrength                                            string    `json:"give_strength"`                                                // RXE.23
	GiveStrengthUnits                                       *CE       `json:"give_strength_units"`                                          // RXE.24
	GiveIndication                                          []*CE     `json:"give_indication"`                                              // RXE.25
	DispensePackageSize                                     string    `json:"dispense_package_size"`                                        // RXE.26
	DispensePackageSizeUnit                                 *CE       `json:"dispense_package_size_unit"`                                   // RXE.27
	DispensePackageMethod                                   string    `json:"dispense_package_method"`                                      // RXE.28
	SupplementaryCode                                       []*CE     `json:"supplementary_code"`                                           // RXE.29
	OriginalOrderDateTime                                   time.Time `json:"original_order_date_time"`                                     // RXE.30
	GiveDrugStrengthVolume                                  string    `json:"give_drug_strength_volume"`                                    // RXE.31
	GiveDrugStrengthVolumeUnits                             *CE       `json:"give_drug_strength_volume_units"`                              // RXE.32
	ControlledSubstanceSchedule                             *CE       `json:"controlled_substance_schedule"`                                // RXE.33
	FormularyStatus                                         string    `json:"formulary_status"`                                             // RXE.34
	PharmaceuticalSubstanceAlternative                      []*CE     `json:"pharmaceutical_substance_alternative"`                         // RXE.35
	PharmacyOfMostRecentFill                                *CE       `json:"pharmacy_of_most_recent_fill"`                                 // RXE.36
	InitialDispenseAmount                                   string    `json:"initial_dispense_amount"`                                      // RXE.37
	DispensingPharmacy                                      *CE       `json:"dispensing_pharmacy"`                                          // RXE.38
	DispensingPharmacyAddress                               *XAD      `json:"dispensing_pharmacy_address"`                                  // RXE.39
	DeliverToPatientLocation                                *PL       `json:"deliver_to_patient_location"`                                  // RXE.40
	DeliverToAddress                                        *XAD      `json:"deliver_to_address"`                                           // RXE.41
}

// RXR - Pharmacy/Treatment Route
type RXR struct {
	Route                      *CE `json:"route"`                        // RXR.1
	AdministrationSite         *CE `json:"administration_site"`          // RXR.2
	AdministrationDevice       *CE `json:"administration_device"`        // RXR.3
	AdministrationMethod       *CE `json:"administration_method"`        // RXR.4
	RoutingInstruction         *CE `json:"routing_instruction"`          // RXR.5
	AdministrationSiteModifier *CE `json:"administration_site_modifier"` // RXR.6
}

// RXC - Pharmacy/Treatment Component Order
type RXC struct {
	RXComponentType                  string `json:"rx_component_type"`                    // RXC.1
	ComponentCode                    *CE    `json:"component_code"`                       // RXC.2
	ComponentAmount                  string `json:"component_amount"`                     // RXC.3
	ComponentUnits                   *CE    `json:"component_units"`                      // RXC.4
	ComponentStrength                string `json:"component_strength"`                   // RXC.5
	ComponentStrengthUnits           *CE    `json:"component_strength_units"`             // RXC.6
	SupplementaryCode                []*CE  `json:"supplementary_code"`                   // RXC.7
	ComponentDrugStrengthVolume      string `json:"component_drug_strength_volume"`       // RXC.8
	ComponentDrugStrengthVolumeUnits *CE    `json:"component_drug_strength_volume_units"` // RXC.9
}

// RXA - Pharmacy/Treatment Administration
type RXA struct {
	GiveSubIDCounter                    string      `json:"give_sub_id_counter"`                     // RXA.1
	AdministrationSubIDCounter          string      `json:"administration_sub_id_counter"`           // RXA.2
	DateTimeStartOfAdministration       time.Time   `json:"date_time_start_of_administration"`       // RXA.3
	DateTimeEndOfAdministration         time.Time   `json:"date_time_end_of_administration"`         // RXA.4
	AdministeredCode                    *CE         `json:"administered_code"`                       // RXA.5
	AdministeredAmount                  string      `json:"administered_amount"`                     // RXA.6
	AdministeredUnits                   *CE         `json:"administered_units"`                      // RXA.7
	AdministeredDosageForm              *CE         `json:"administered_dosage_form"`                // RXA.8
	AdministrationNotes                 []*CE       `json:"administration_notes"`                    // RXA.9
	AdministeringProvider               []*XCN      `json:"administering_provider"`                  // RXA.10
	AdministeredAtLocation              *PL         `json:"administered_at_location"`                // RXA.11
	AdministeredPerTimeUnit             string      `json:"administered_per_time_unit"`              // RXA.12
	AdministeredStrength                string      `json:"administered_strength"`                   // RXA.13
	AdministeredStrengthUnits           *CE         `json:"administered_strength_units"`             // RXA.14
	SubstanceLotNumber                  []string    `json:"substance_lot_number"`                    // RXA.15
	SubstanceExpirationDate             []time.Time `json:"substance_expiration_date"`               // RXA.16
	SubstanceManufacturerName           []*CE       `json:"substance_manufacturer_name"`             // RXA.17
	SubstanceRefusalReason              []*CE       `json:"substance_refusal_reason"`                // RXA.18
	Indication                          []*CE       `json:"indication"`                              // RXA.19
	CompletionStatus                    string      `json:"completion_status"`                       // RXA.20
	ActionCodeRXA                       string      `json:"action_code_rxa"`                         // RXA.21
	SystemEntryDateTime                 time.Time   `json:"system_entry_date_time"`                  // RXA.22
	AdministeredDrugStrengthVolume      string      `json:"administered_drug_strength_volume"`       // RXA.23
	AdministeredDrugStrengthVolumeUnits *CE         `json:"administered_drug_strength_volume_units"` // RXA.24
	AdministeredBarcodeIdentifier       string      `json:"administered_barcode_identifier"`         // RXA.25
	PharmacyOrderType                   string      `json:"pharmacy_order_type"`                     // RXA.26
}
