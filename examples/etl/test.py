#!/usr/bin/env python3
"""
HL7 MLLP Test Sender
Sends test HL7 messages to BridgeLink for testing the parser
"""

import socket
import time
from datetime import datetime

# MLLP framing characters
START_BLOCK = b'\x0B'  # Vertical Tab
END_BLOCK = b'\x1C'    # File Separator
CARRIAGE_RETURN = b'\x0D'  # Carriage Return

def send_hl7_message(host, port, hl7_message):
    """Send an HL7 message using MLLP protocol"""
    try:
        # Create socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10)

        # Connect
        print(f"Connecting to {host}:{port}...")
        sock.connect((host, port))
        print("Connected!")

        # Prepare message with MLLP framing
        message = START_BLOCK + hl7_message.encode('utf-8') + END_BLOCK + CARRIAGE_RETURN

        # Send message
        print(f"\nSending message ({len(hl7_message)} bytes)...")
        sock.send(message)
        print("Message sent!")

        # Receive acknowledgment
        print("\nWaiting for ACK...")
        ack = sock.recv(4096)

        # Remove MLLP framing from ACK
        if ack.startswith(START_BLOCK):
            ack = ack[1:]
        if ack.endswith(CARRIAGE_RETURN):
            ack = ack[:-1]
        if ack.endswith(END_BLOCK):
            ack = ack[:-1]

        print(f"ACK Received:\n{ack.decode('utf-8')}")

        # Close connection
        sock.close()
        return True

    except Exception as e:
        print(f"Error: {e}")
        return False

def get_timestamp():
    """Get current timestamp in HL7 format"""
    return datetime.now().strftime("%Y%m%d%H%M%S")

# Test HL7 Messages
test_messages = {
    "ADT_A01 - Patient Admission": """MSH|^~\\&|SendingApp|SendingFac|ReceivingApp|ReceivingFac|{timestamp}||ADT^A01^ADT_A01|MSG{seq}|P|2.5
EVN|A01|{timestamp}|||UserID^UserName^UserRole
PID|1||12345^^^Hospital^MR||Doe^John^A^Jr^Dr||19800101|M|||123 Main St^Apt 4B^Springfield^IL^62701^USA^H||555-1234^PRN^PH^^^555^1234~555-5678^WPN^PH|||M|NON|987654321|||N|Birth Place||||N
NK1|1|Doe^Jane^A|SPO|456 Oak Ave^^Springfield^IL^62701|555-9999^PRN^PH|||20100101
PV1|1|I|W^389^1^Hospital^^ICU^2|||2234^Doctor^Attending^A|||SUR||||ADM|A0|||2234^Doctor^Attending^A|SUR|10001^VisitNum|4||||||||||||||||||||||||{timestamp}
AL1|1|DA|1234^Penicillin^RxNorm|SV|Rash~Hives
DG1|1|I10|I21.9^Acute myocardial infarction^ICD10||{timestamp}|F
IN1|1|PLAN001|INS001|Insurance Company Name|PO Box 12345^^Springfield^IL^62701|||GRP12345|Group Name|||||||||Doe^John^A|SEL||||||||||||||||||12345678||||||||||||||M""",

    "ORU_R01 - Lab Results": """MSH|^~\\&|LAB|Hospital|LIS|Hospital|{timestamp}||ORU^R01^ORU_R01|MSG{seq}|P|2.5
PID|1||12345^^^Hospital^MR||Doe^John^A||19800101|M|||123 Main St^^Springfield^IL^62701
OBR|1|ORD123456|LAB123456|CBC^Complete Blood Count^LN|||{timestamp}|{timestamp}||||||Specimen received|2234^Doctor^Ordering|||||||{timestamp}|||F
OBX|1|NM|WBC^White Blood Count^LN||7.5|10*9/L|4.0-11.0|N|||F|||{timestamp}
OBX|2|NM|RBC^Red Blood Count^LN||4.5|10*12/L|4.2-5.4|N|||F|||{timestamp}
OBX|3|NM|HGB^Hemoglobin^LN||14.5|g/dL|13.0-17.0|N|||F|||{timestamp}
OBX|4|NM|HCT^Hematocrit^LN||42.0|%|38.0-50.0|N|||F|||{timestamp}
OBX|5|NM|PLT^Platelet Count^LN||250|10*9/L|150-400|N|||F|||{timestamp}
OBX|6|ST|COMMENT^Comment^LN||Patient fasting status: 12 hours||||F|||{timestamp}""",

    "ADT_A08 - Patient Update": """MSH|^~\\&|SendingApp|SendingFac|ReceivingApp|ReceivingFac|{timestamp}||ADT^A08^ADT_A08|MSG{seq}|P|2.5
EVN|A08|{timestamp}
PID|1||12345^^^Hospital^MR||Doe^John^A||19800101|M|||456 New Address St^Suite 100^Springfield^IL^62702^USA^H||555-4321^PRN^PH|||M
PV1|1|O|CLINIC^101^1|||2234^Doctor^Attending|||MED||||REG|A0""",

    "ORM_O01 - Medication Order": """MSH|^~\\&|OrderEntry|Hospital|Pharmacy|Hospital|{timestamp}||ORM^O01^ORM_O01|MSG{seq}|P|2.5
PID|1||12345^^^Hospital^MR||Doe^John^A||19800101|M|||123 Main St^^Springfield^IL^62701
ORC|NW|ORD{seq}|||||^^^{timestamp}||{timestamp}|||2234^Doctor^Ordering
RXO|00054001200^Aspirin 325mg TAB^NDC|1||TAB|||||G||5|refills||||||||||||||||Chest Pain
RXR|PO^Oral^HL70162""",

    "SIU_S12 - Appointment Notification": """MSH|^~\\&|Scheduling|Hospital|Clinic|Hospital|{timestamp}||SIU^S12^SIU_S12|MSG{seq}|P|2.5
SCH|{seq}||||||Consultation^Office Visit^SCHED_TYPE|30|min|^^^20231120100000||||||2234^Doctor^Attending||||||Scheduled
PID|1||12345^^^Hospital^MR||Doe^John^A||19800101|M|||123 Main St^^Springfield^IL^62701||555-1234
RGS|1|A
AIG|1||2234^Doctor^Attending||^^^20231120100000|30|min|Confirmed
AIL|1||CLINIC^Room 101^Hospital||^^^20231120100000|30|min|Confirmed""",

    "DFT_P03 - Billing Information": """MSH|^~\\&|Finance|Hospital|Billing|Hospital|{timestamp}||DFT^P03^DFT_P03|MSG{seq}|P|2.5
EVN|P03|{timestamp}
PID|1||12345^^^Hospital^MR||Doe^John^A||19800101|M|||123 Main St^^Springfield^IL^62701
FT1|1||{timestamp}|{timestamp}|CG|99213^Office Visit^CPT|||1|200.00|USD||||Outpatient||2234^Doctor^Attending
FT1|2||{timestamp}|{timestamp}|CG|85025^Complete Blood Count^CPT|||1|45.00|USD||||Laboratory||2234^Doctor^Ordering""",

    "MDM_T02 - Document Notification": """MSH|^~\\&|HIS|Hospital|DocSystem|Hospital|{timestamp}||MDM^T02^MDM_T02|MSG{seq}|P|2.5
EVN|T02|{timestamp}
PID|1||12345^^^Hospital^MR||Doe^John^A||19800101|M
PV1|1|I|W^389^1|||2234^Doctor^Attending
TXA|1|DS|TX|{timestamp}||{timestamp}|{timestamp}|||2234^Doctor^Attending^Signing||DOC{seq}||||PA||AV
OBX|1|TX|REPORT^Report Text^LOCAL||Discharge Summary: Patient admitted for chest pain. Diagnosis: Acute MI. Treatment provided. Patient stable and ready for discharge.||||||F""",

    "VXU_V04 - Vaccination Record": """MSH|^~\\&|IIS|Hospital|Registry|State|{timestamp}||VXU^V04^VXU_V04|MSG{seq}|P|2.5
PID|1||12345^^^Hospital^MR||Doe^John^A||19800101|M|||123 Main St^^Springfield^IL^62701
ORC|RE||||||^^^{timestamp}
RXA|0|1|{timestamp}|{timestamp}|208^Fluzone Quadrivalent^CVX|0.5|mL|00|01^Historical^NIP001||||||12345|{timestamp}|||CP|A
RXR|IM^Intramuscular^HL70162|LA^Left Arm^HL70163
OBX|1|CE|30963-3^Vaccine funding source^LN||V02^Other^HL70064||||||F
OBX|2|TS|29768-9^Date vaccine information statement published^LN||20230701||||||F""",
}

def main():
    host = "localhost"
    port = 2575

    print("=" * 80)
    print("HL7 MLLP Test Sender for BridgeLink")
    print("=" * 80)
    print(f"\nTarget: {host}:{port}")
    print(f"Available test messages: {len(test_messages)}\n")

    # Interactive mode
    while True:
        print("\n" + "=" * 80)
        print("Test Messages:")
        print("=" * 80)
        for idx, (name, _) in enumerate(test_messages.items(), 1):
            print(f"{idx}. {name}")
        print(f"{len(test_messages) + 1}. Send All Messages")
        print("0. Exit")
        print("=" * 80)

        try:
            choice = input("\nSelect message to send (0-{}): ".format(len(test_messages) + 1))
            choice = int(choice)

            if choice == 0:
                print("\nExiting...")
                break
            elif choice == len(test_messages) + 1:
                # Send all messages
                print("\n🚀 Sending all test messages...\n")
                for idx, (name, message_template) in enumerate(test_messages.items(), 1):
                    print(f"\n{'='*60}")
                    print(f"Message {idx}/{len(test_messages)}: {name}")
                    print('='*60)

                    message = message_template.format(
                        timestamp=get_timestamp(),
                        seq=str(idx).zfill(5)
                    )

                    success = send_hl7_message(host, port, message)
                    if success:
                        print("✓ Success")
                    else:
                        print("✗ Failed")

                    if idx < len(test_messages):
                        time.sleep(1)  # Wait between messages

                print(f"\n✓ Completed sending {len(test_messages)} messages!")

            elif 1 <= choice <= len(test_messages):
                # Send selected message
                name = list(test_messages.keys())[choice - 1]
                message_template = test_messages[name]

                print(f"\n🚀 Sending: {name}\n")
                print("="*60)

                message = message_template.format(
                    timestamp=get_timestamp(),
                    seq=str(choice).zfill(5)
                )

                print("HL7 Message Preview:")
                print("-"*60)
                lines = message.split('\n')
                for line in lines[:5]:  # Show first 5 lines
                    print(line)
                if len(lines) > 5:
                    print(f"... ({len(lines) - 5} more lines)")
                print("-"*60)

                success = send_hl7_message(host, port, message)
                if success:
                    print("\n✓ Message sent successfully!")
                else:
                    print("\n✗ Failed to send message")
            else:
                print("Invalid choice!")

        except ValueError:
            print("Invalid input! Please enter a number.")
        except KeyboardInterrupt:
            print("\n\nInterrupted by user. Exiting...")
            break
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    main()
