package parsers

import (
	"time"
)

// ADT_A01 - Admit/Visit Notification
type ADT_A01 struct {
	MSH *MSH   `json:"msh"` // Message Header
	EVN *EVN   `json:"evn"` // Event Type
	PID *PID   `json:"pid"` // Patient Identification
	NK1 []*NK1 `json:"nk1"` // Next of Kin / Associated Parties
	PV1 *PV1   `json:"pv1"` // Patient Visit
	PV2 *PV2   `json:"pv2"` // Patient Visit - Additional Information
	AL1 []*AL1 `json:"al1"` // Patient Allergy Information
	DG1 []*DG1 `json:"dg1"` // Diagnosis
	PR1 []*PR1 `json:"pr1"` // Procedures
	ROL []*ROL `json:"rol"` // Role
}

func (m *ADT_A01) GetMessageType() string {
	if m.MSH != nil && m.MSH.MessageType != nil {
		return m.MSH.MessageType.MessageCode + "^" + m.MSH.MessageType.TriggerEvent
	}
	return "ADT^A01"
}

func (m *ADT_A01) GetMessageControlID() string {
	if m.MSH != nil {
		return m.MSH.MessageControlID
	}
	return ""
}

func (m *ADT_A01) GetTimestamp() time.Time {
	if m.MSH != nil {
		return m.MSH.DateTimeOfMessage
	}
	return time.Time{}
}

// ADT_A02 - Transfer a Patient
type ADT_A02 struct {
	MSH *MSH `json:"msh"` // Message Header
	EVN *EVN `json:"evn"` // Event Type
	PID *PID `json:"pid"` // Patient Identification
	PV1 *PV1 `json:"pv1"` // Patient Visit
	PV2 *PV2 `json:"pv2"` // Patient Visit - Additional Information
}

func (m *ADT_A02) GetMessageType() string {
	if m.MSH != nil && m.MSH.MessageType != nil {
		return m.MSH.MessageType.MessageCode + "^" + m.MSH.MessageType.TriggerEvent
	}
	return "ADT^A02"
}

func (m *ADT_A02) GetMessageControlID() string {
	if m.MSH != nil {
		return m.MSH.MessageControlID
	}
	return ""
}

func (m *ADT_A02) GetTimestamp() time.Time {
	if m.MSH != nil {
		return m.MSH.DateTimeOfMessage
	}
	return time.Time{}
}

// ADT_A03 - Discharge/End Visit
type ADT_A03 struct {
	MSH *MSH   `json:"msh"` // Message Header
	EVN *EVN   `json:"evn"` // Event Type
	PID *PID   `json:"pid"` // Patient Identification
	PV1 *PV1   `json:"pv1"` // Patient Visit
	PV2 *PV2   `json:"pv2"` // Patient Visit - Additional Information
	AL1 []*AL1 `json:"al1"` // Patient Allergy Information
	DG1 []*DG1 `json:"dg1"` // Diagnosis
}

func (m *ADT_A03) GetMessageType() string {
	if m.MSH != nil && m.MSH.MessageType != nil {
		return m.MSH.MessageType.MessageCode + "^" + m.MSH.MessageType.TriggerEvent
	}
	return "ADT^A03"
}

func (m *ADT_A03) GetMessageControlID() string {
	if m.MSH != nil {
		return m.MSH.MessageControlID
	}
	return ""
}

func (m *ADT_A03) GetTimestamp() time.Time {
	if m.MSH != nil {
		return m.MSH.DateTimeOfMessage
	}
	return time.Time{}
}

// ADT_A04 - Register a Patient
type ADT_A04 struct {
	MSH *MSH   `json:"msh"` // Message Header
	EVN *EVN   `json:"evn"` // Event Type
	PID *PID   `json:"pid"` // Patient Identification
	NK1 []*NK1 `json:"nk1"` // Next of Kin / Associated Parties
	PV1 *PV1   `json:"pv1"` // Patient Visit
	PV2 *PV2   `json:"pv2"` // Patient Visit - Additional Information
	AL1 []*AL1 `json:"al1"` // Patient Allergy Information
	DG1 []*DG1 `json:"dg1"` // Diagnosis
	PR1 []*PR1 `json:"pr1"` // Procedures
	ROL []*ROL `json:"rol"` // Role
}

func (m *ADT_A04) GetMessageType() string {
	if m.MSH != nil && m.MSH.MessageType != nil {
		return m.MSH.MessageType.MessageCode + "^" + m.MSH.MessageType.TriggerEvent
	}
	return "ADT^A04"
}

func (m *ADT_A04) GetMessageControlID() string {
	if m.MSH != nil {
		return m.MSH.MessageControlID
	}
	return ""
}

func (m *ADT_A04) GetTimestamp() time.Time {
	if m.MSH != nil {
		return m.MSH.DateTimeOfMessage
	}
	return time.Time{}
}

// ADT_A05 - Pre-admit a Patient
type ADT_A05 struct {
	MSH *MSH   `json:"msh"` // Message Header
	EVN *EVN   `json:"evn"` // Event Type
	PID *PID   `json:"pid"` // Patient Identification
	NK1 []*NK1 `json:"nk1"` // Next of Kin / Associated Parties
	PV1 *PV1   `json:"pv1"` // Patient Visit
	PV2 *PV2   `json:"pv2"` // Patient Visit - Additional Information
	AL1 []*AL1 `json:"al1"` // Patient Allergy Information
	DG1 []*DG1 `json:"dg1"` // Diagnosis
	PR1 []*PR1 `json:"pr1"` // Procedures
	ROL []*ROL `json:"rol"` // Role
}

func (m *ADT_A05) GetMessageType() string {
	if m.MSH != nil && m.MSH.MessageType != nil {
		return m.MSH.MessageType.MessageCode + "^" + m.MSH.MessageType.TriggerEvent
	}
	return "ADT^A05"
}

func (m *ADT_A05) GetMessageControlID() string {
	if m.MSH != nil {
		return m.MSH.MessageControlID
	}
	return ""
}

func (m *ADT_A05) GetTimestamp() time.Time {
	if m.MSH != nil {
		return m.MSH.DateTimeOfMessage
	}
	return time.Time{}
}

// ADT_A06 - Change an Outpatient to an Inpatient
type ADT_A06 struct {
	MSH *MSH   `json:"msh"` // Message Header
	EVN *EVN   `json:"evn"` // Event Type
	PID *PID   `json:"pid"` // Patient Identification
	NK1 []*NK1 `json:"nk1"` // Next of Kin / Associated Parties
	PV1 *PV1   `json:"pv1"` // Patient Visit
	PV2 *PV2   `json:"pv2"` // Patient Visit - Additional Information
	AL1 []*AL1 `json:"al1"` // Patient Allergy Information
	DG1 []*DG1 `json:"dg1"` // Diagnosis
	PR1 []*PR1 `json:"pr1"` // Procedures
	ROL []*ROL `json:"rol"` // Role
}

func (m *ADT_A06) GetMessageType() string {
	if m.MSH != nil && m.MSH.MessageType != nil {
		return m.MSH.MessageType.MessageCode + "^" + m.MSH.MessageType.TriggerEvent
	}
	return "ADT^A06"
}

func (m *ADT_A06) GetMessageControlID() string {
	if m.MSH != nil {
		return m.MSH.MessageControlID
	}
	return ""
}

func (m *ADT_A06) GetTimestamp() time.Time {
	if m.MSH != nil {
		return m.MSH.DateTimeOfMessage
	}
	return time.Time{}
}

// ADT_A08 - Update Patient Information
type ADT_A08 struct {
	MSH *MSH   `json:"msh"` // Message Header
	EVN *EVN   `json:"evn"` // Event Type
	PID *PID   `json:"pid"` // Patient Identification
	NK1 []*NK1 `json:"nk1"` // Next of Kin / Associated Parties
	PV1 *PV1   `json:"pv1"` // Patient Visit
	PV2 *PV2   `json:"pv2"` // Patient Visit - Additional Information
	AL1 []*AL1 `json:"al1"` // Patient Allergy Information
	DG1 []*DG1 `json:"dg1"` // Diagnosis
	PR1 []*PR1 `json:"pr1"` // Procedures
	ROL []*ROL `json:"rol"` // Role
}

func (m *ADT_A08) GetMessageType() string {
	if m.MSH != nil && m.MSH.MessageType != nil {
		return m.MSH.MessageType.MessageCode + "^" + m.MSH.MessageType.TriggerEvent
	}
	return "ADT^A08"
}

func (m *ADT_A08) GetMessageControlID() string {
	if m.MSH != nil {
		return m.MSH.MessageControlID
	}
	return ""
}

func (m *ADT_A08) GetTimestamp() time.Time {
	if m.MSH != nil {
		return m.MSH.DateTimeOfMessage
	}
	return time.Time{}
}

// ADT_A11 - Cancel Admit/Visit Notification
type ADT_A11 struct {
	MSH *MSH `json:"msh"` // Message Header
	EVN *EVN `json:"evn"` // Event Type
	PID *PID `json:"pid"` // Patient Identification
	PV1 *PV1 `json:"pv1"` // Patient Visit
	PV2 *PV2 `json:"pv2"` // Patient Visit - Additional Information
}

func (m *ADT_A11) GetMessageType() string {
	if m.MSH != nil && m.MSH.MessageType != nil {
		return m.MSH.MessageType.MessageCode + "^" + m.MSH.MessageType.TriggerEvent
	}
	return "ADT^A11"
}

func (m *ADT_A11) GetMessageControlID() string {
	if m.MSH != nil {
		return m.MSH.MessageControlID
	}
	return ""
}

func (m *ADT_A11) GetTimestamp() time.Time {
	if m.MSH != nil {
		return m.MSH.DateTimeOfMessage
	}
	return time.Time{}
}

// ADT_A12 - Cancel Transfer
type ADT_A12 struct {
	MSH *MSH `json:"msh"` // Message Header
	EVN *EVN `json:"evn"` // Event Type
	PID *PID `json:"pid"` // Patient Identification
	PV1 *PV1 `json:"pv1"` // Patient Visit
	PV2 *PV2 `json:"pv2"` // Patient Visit - Additional Information
}

func (m *ADT_A12) GetMessageType() string {
	if m.MSH != nil && m.MSH.MessageType != nil {
		return m.MSH.MessageType.MessageCode + "^" + m.MSH.MessageType.TriggerEvent
	}
	return "ADT^A12"
}

func (m *ADT_A12) GetMessageControlID() string {
	if m.MSH != nil {
		return m.MSH.MessageControlID
	}
	return ""
}

func (m *ADT_A12) GetTimestamp() time.Time {
	if m.MSH != nil {
		return m.MSH.DateTimeOfMessage
	}
	return time.Time{}
}

// ADT_A13 - Cancel Discharge/End Visit
type ADT_A13 struct {
	MSH *MSH `json:"msh"` // Message Header
	EVN *EVN `json:"evn"` // Event Type
	PID *PID `json:"pid"` // Patient Identification
	PV1 *PV1 `json:"pv1"` // Patient Visit
	PV2 *PV2 `json:"pv2"` // Patient Visit - Additional Information
}

func (m *ADT_A13) GetMessageType() string {
	if m.MSH != nil && m.MSH.MessageType != nil {
		return m.MSH.MessageType.MessageCode + "^" + m.MSH.MessageType.TriggerEvent
	}
	return "ADT^A13"
}

func (m *ADT_A13) GetMessageControlID() string {
	if m.MSH != nil {
		return m.MSH.MessageControlID
	}
	return ""
}

func (m *ADT_A13) GetTimestamp() time.Time {
	if m.MSH != nil {
		return m.MSH.DateTimeOfMessage
	}
	return time.Time{}
}

// ORU_R01 - Unsolicited Observation Message
type ORU_R01 struct {
	MSH *MSH   `json:"msh"` // Message Header
	PID *PID   `json:"pid"` // Patient Identification
	PV1 *PV1   `json:"pv1"` // Patient Visit
	ORC []*ORC `json:"orc"` // Common Order
	OBR []*OBR `json:"obr"` // Observation Request
	OBX []*OBX `json:"obx"` // Observation/Result
	NTE []*NTE `json:"nte"` // Notes and Comments
	ROL []*ROL `json:"rol"` // Role
}

func (m *ORU_R01) GetMessageType() string {
	if m.MSH != nil && m.MSH.MessageType != nil {
		return m.MSH.MessageType.MessageCode + "^" + m.MSH.MessageType.TriggerEvent
	}
	return "ORU^R01"
}

func (m *ORU_R01) GetMessageControlID() string {
	if m.MSH != nil {
		return m.MSH.MessageControlID
	}
	return ""
}

func (m *ORU_R01) GetTimestamp() time.Time {
	if m.MSH != nil {
		return m.MSH.DateTimeOfMessage
	}
	return time.Time{}
}

// NTE - Notes and Comments
type NTE struct {
	SetID           string   `json:"set_id"`            // NTE.1
	SourceOfComment string   `json:"source_of_comment"` // NTE.2
	Comment         []string `json:"comment"`           // NTE.3
	CommentType     *CE      `json:"comment_type"`      // NTE.4
}

// ORM_O01 - Pharmacy/Treatment Order Message
type ORM_O01 struct {
	MSH *MSH   `json:"msh"` // Message Header
	PID *PID   `json:"pid"` // Patient Identification
	PV1 *PV1   `json:"pv1"` // Patient Visit
	ORC *ORC   `json:"orc"` // Common Order
	RXE *RXE   `json:"rxe"` // Pharmacy/Treatment Encoded Order
	RXR []*RXR `json:"rxr"` // Pharmacy/Treatment Route
	RXC []*RXC `json:"rxc"` // Pharmacy/Treatment Component Order
	ROL []*ROL `json:"rol"` // Role
}

func (m *ORM_O01) GetMessageType() string {
	if m.MSH != nil && m.MSH.MessageType != nil {
		return m.MSH.MessageType.MessageCode + "^" + m.MSH.MessageType.TriggerEvent
	}
	return "ORM^O01"
}

func (m *ORM_O01) GetMessageControlID() string {
	if m.MSH != nil {
		return m.MSH.MessageControlID
	}
	return ""
}

func (m *ORM_O01) GetTimestamp() time.Time {
	if m.MSH != nil {
		return m.MSH.DateTimeOfMessage
	}
	return time.Time{}
}

// ORR_O02 - Pharmacy/Treatment Order Response
type ORR_O02 struct {
	MSH *MSH   `json:"msh"` // Message Header
	MSA *MSA   `json:"msa"` // Message Acknowledgment
	PID *PID   `json:"pid"` // Patient Identification
	PV1 *PV1   `json:"pv1"` // Patient Visit
	ORC *ORC   `json:"orc"` // Common Order
	RXE *RXE   `json:"rxe"` // Pharmacy/Treatment Encoded Order
	RXR []*RXR `json:"rxr"` // Pharmacy/Treatment Route
	RXC []*RXC `json:"rxc"` // Pharmacy/Treatment Component Order
	ROL []*ROL `json:"rol"` // Role
}

func (m *ORR_O02) GetMessageType() string {
	if m.MSH != nil && m.MSH.MessageType != nil {
		return m.MSH.MessageType.MessageCode + "^" + m.MSH.MessageType.TriggerEvent
	}
	return "ORR^O02"
}

func (m *ORR_O02) GetMessageControlID() string {
	if m.MSH != nil {
		return m.MSH.MessageControlID
	}
	return ""
}

func (m *ORR_O02) GetTimestamp() time.Time {
	if m.MSH != nil {
		return m.MSH.DateTimeOfMessage
	}
	return time.Time{}
}

// RAS_O17 - Pharmacy/Treatment Administration Message
type RAS_O17 struct {
	MSH *MSH   `json:"msh"` // Message Header
	PID *PID   `json:"pid"` // Patient Identification
	PV1 *PV1   `json:"pv1"` // Patient Visit
	ORC *ORC   `json:"orc"` // Common Order
	RXE *RXE   `json:"rxe"` // Pharmacy/Treatment Encoded Order
	RXR []*RXR `json:"rxr"` // Pharmacy/Treatment Route
	RXC []*RXC `json:"rxc"` // Pharmacy/Treatment Component Order
	RXA []*RXA `json:"rxa"` // Pharmacy/Treatment Administration
	ROL []*ROL `json:"rol"` // Role
}

func (m *RAS_O17) GetMessageType() string {
	if m.MSH != nil && m.MSH.MessageType != nil {
		return m.MSH.MessageType.MessageCode + "^" + m.MSH.MessageType.TriggerEvent
	}
	return "RAS^O17"
}

func (m *RAS_O17) GetMessageControlID() string {
	if m.MSH != nil {
		return m.MSH.MessageControlID
	}
	return ""
}

func (m *RAS_O17) GetTimestamp() time.Time {
	if m.MSH != nil {
		return m.MSH.DateTimeOfMessage
	}
	return time.Time{}
}

// ACK - General Acknowledgment
type ACK struct {
	MSH *MSH   `json:"msh"` // Message Header
	MSA *MSA   `json:"msa"` // Message Acknowledgment
	ERR []*ERR `json:"err"` // Error
}

func (m *ACK) GetMessageType() string {
	if m.MSH != nil && m.MSH.MessageType != nil {
		return m.MSH.MessageType.MessageCode + "^" + m.MSH.MessageType.TriggerEvent
	}
	return "ACK"
}

func (m *ACK) GetMessageControlID() string {
	if m.MSH != nil {
		return m.MSH.MessageControlID
	}
	return ""
}

func (m *ACK) GetTimestamp() time.Time {
	if m.MSH != nil {
		return m.MSH.DateTimeOfMessage
	}
	return time.Time{}
}

// SIU_S12 - Notification of New Appointment Booking
type SIU_S12 struct {
	MSH *MSH   `json:"msh"` // Message Header
	SCH *SCH   `json:"sch"` // Scheduling Activity Information
	RGS []*RGS `json:"rgs"` // Resource Group
	AIG []*AIG `json:"aig"` // Appointment Information - General Resource
	AIL []*AIL `json:"ail"` // Appointment Information - Location Resource
	AIS []*AIS `json:"ais"` // Appointment Information - Service
	PID *PID   `json:"pid"` // Patient Identification
	PV1 *PV1   `json:"pv1"` // Patient Visit
	ROL []*ROL `json:"rol"` // Role
}

func (m *SIU_S12) GetMessageType() string {
	if m.MSH != nil && m.MSH.MessageType != nil {
		return m.MSH.MessageType.MessageCode + "^" + m.MSH.MessageType.TriggerEvent
	}
	return "SIU^S12"
}

func (m *SIU_S12) GetMessageControlID() string {
	if m.MSH != nil {
		return m.MSH.MessageControlID
	}
	return ""
}

func (m *SIU_S12) GetTimestamp() time.Time {
	if m.MSH != nil {
		return m.MSH.DateTimeOfMessage
	}
	return time.Time{}
}

// DFT_P03 - Post Detail Financial Transaction
type DFT_P03 struct {
	MSH *MSH   `json:"msh"` // Message Header
	EVN *EVN   `json:"evn"` // Event Type
	PID *PID   `json:"pid"` // Patient Identification
	PV1 *PV1   `json:"pv1"` // Patient Visit
	FT1 []*FT1 `json:"ft1"` // Financial Transaction
	ROL []*ROL `json:"rol"` // Role
}

func (m *DFT_P03) GetMessageType() string {
	if m.MSH != nil && m.MSH.MessageType != nil {
		return m.MSH.MessageType.MessageCode + "^" + m.MSH.MessageType.TriggerEvent
	}
	return "DFT^P03"
}

func (m *DFT_P03) GetMessageControlID() string {
	if m.MSH != nil {
		return m.MSH.MessageControlID
	}
	return ""
}

func (m *DFT_P03) GetTimestamp() time.Time {
	if m.MSH != nil {
		return m.MSH.DateTimeOfMessage
	}
	return time.Time{}
}
