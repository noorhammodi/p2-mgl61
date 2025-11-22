import uuid
import threading
from datetime import datetime

from flask import Flask, render_template, request, redirect, url_for, jsonify

from common import (
    create_producer,
    create_consumer,
    LAB_TEST_ORDERS_TOPIC,
    LAB_RESULTS_TOPIC,
)

app = Flask(__name__)

# Kafka producer unique pour l’appli web
producer = create_producer()

# Mémoire en RAM pour les notifications
producer_notifications = []   # notifications du producteur (stimulus envoyé)
consumer_notifications = []   # notifications des consommateurs (lab + résultats)


def _append_notification(target_list, message: str):
    """Add a notification with timestamp and keep only the last 50."""
    target_list.append(
        {
            "timestamp": datetime.utcnow().isoformat(timespec="seconds"),
            "message": message,
        }
    )
    if len(target_list) > 50:
        del target_list[0]


def orders_consumer_worker():
    """
    Consommateur pour les ordres de tests (vu côté lab).
    Écoute le topic LAB_TEST_ORDERS_TOPIC.
    """
    consumer = create_consumer(
        topic=LAB_TEST_ORDERS_TOPIC,
        group_id="web-ui-lab-orders",
    )
    for msg in consumer:
        event = msg.value
        _append_notification(
            consumer_notifications,
            f"[LAB - ORDRE REÇU] Patient: {event.get('patient_name')} | "
            f"Test: {event.get('test_type')} | "
            f"order_id={event.get('order_id')}",
        )


def results_consumer_worker():
    """
    Consommateur pour les résultats de lab (vu côté médecin / patient).
    Écoute le topic LAB_RESULTS_TOPIC.
    """
    consumer = create_consumer(
        topic=LAB_RESULTS_TOPIC,
        group_id="web-ui-lab-results",
    )
    for msg in consumer:
        event = msg.value
        _append_notification(
            consumer_notifications,
            f"[RÉSULTAT LAB] Patient: {event.get('patient_name')} | "
            f"Test: {event.get('test_type')} | "
            f"Résultat: {event.get('result')}",
        )


def start_background_threads():
    """Lancement des threads Kafka consumers en arrière-plan."""
    threading.Thread(target=orders_consumer_worker, daemon=True).start()
    threading.Thread(target=results_consumer_worker, daemon=True).start()


@app.route("/", methods=["GET"])
def index():
    # On passe les listes à la page (du plus récent au plus ancien)
    return render_template(
        "index.html",
        producer_notifications=reversed(producer_notifications),
        consumer_notifications=reversed(consumer_notifications),
    )


@app.route("/stimulus", methods=["POST"])
def stimulus():
    """
    Petit formulaire pour envoyer un 'stimulus' = un ordre de lab.
    Cela joue le rôle de producteur.
    """
    patient_name = request.form.get("patient_name", "").strip()
    test_type = request.form.get("test_type", "").strip()

    if not patient_name or not test_type:
        # Pour simplifier, on revient juste à la page d’accueil
        return redirect(url_for("index"))

    order_id = str(uuid.uuid4())

    event = {
        "order_id": order_id,
        "patient_name": patient_name,
        "test_type": test_type,
        "source": "web-ui",
        "priority": "normal",
        "ordered_at": datetime.utcnow().isoformat(),
    }

    # Envoi sur Kafka
    producer.send(LAB_TEST_ORDERS_TOPIC, value=event)
    producer.flush()

    _append_notification(
        producer_notifications,
        f"[ORDRE ENVOYÉ] Patient: {patient_name} | Test: {test_type} | "
        f"order_id={order_id}",
    )

    return redirect(url_for("index"))


@app.route("/notifications", methods=["GET"])
def notifications():
    """
    Endpoint JSON pour rafraîchir les notifications en AJAX.
    """
    return jsonify(
        producer=list(reversed(producer_notifications)),
        consumer=list(reversed(consumer_notifications)),
    )


if __name__ == "__main__":
    start_background_threads()
    app.run(host="0.0.0.0", port=5000, debug=True)
