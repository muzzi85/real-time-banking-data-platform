from kafka import KafkaConsumer

import ollama

import json


consumer = KafkaConsumer(

    "fraud-alerts",

    bootstrap_servers="localhost:9092",

    auto_offset_reset="latest",

    value_deserializer=lambda m:
    json.loads(m.decode("utf-8"))
)


print("Fraud Monitoring Agent Started...")


for message in consumer:

    fraud_event = message.value

    print("\nReceived Fraud Alert:")
    print(fraud_event)


    fraud_reason = fraud_event["fraud_reason"]


    if fraud_reason == "HIGH_AMOUNT":

        reason_text = (
            "Transaction exceeded configured fraud threshold of 4000."
        )

        action_text = (
            "Recommend analyst review and customer verification."
        )

    else:

        reason_text = (
            "Unknown fraud pattern detected."
        )

        action_text = (
            "Recommend manual investigation."
        )


    prompt = f"""

    Fraud Event:
    {json.dumps(fraud_event, indent=2)}

    Verified Fraud Reason:
    {reason_text}

    Recommended Action:
    {action_text}

    Rules:
    - Do NOT invent information
    - Maximum 3 lines
    - Keep concise and professional

    """


    response = ollama.chat(

        model="tinyllama",

        messages=[

            {
                "role": "user",
                "content": prompt
            }
        ]
    )


    explanation = response["message"]["content"]


    print("\nAI Fraud Analysis:")
    print(explanation)

    print("\n" + "=" * 80)