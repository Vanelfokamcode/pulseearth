# infra/init_topics.py
# Lance avec : python infra/init_topics.py

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
import time

BROKER = "localhost:9092"

TOPICS = [
    NewTopic(name="earthquakes", num_partitions=3, replication_factor=1),
    NewTopic(name="wildfires",   num_partitions=3, replication_factor=1),
    NewTopic(name="pollution",   num_partitions=3, replication_factor=1),
]

def init_topics():
    print("Connexion à Redpanda...")
    time.sleep(2)  # petite attente au cas où Redpanda vient de démarrer

    admin = KafkaAdminClient(bootstrap_servers=BROKER)

    for topic in TOPICS:
        try:
            admin.create_topics([topic])
            print(f"  ✅ Topic créé : {topic.name} ({topic.num_partitions} partitions)")
        except TopicAlreadyExistsError:
            print(f"  ⚠️  Topic existant : {topic.name} — skip")

    admin.close()
    print("\nTopics disponibles :")
    import subprocess
    subprocess.run(
        ["docker", "exec", "pulseearth-redpanda", "rpk", "topic", "list"],
    )

if __name__ == "__main__":
    init_topics()