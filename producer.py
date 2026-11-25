from contextlib import asynccontextmanager
from threading import Event, Thread

from fastapi import FastAPI, HTTPException, Request

from app.common import enabled, log
from app.events import MovieEvent
from app.generator import generate


@asynccontextmanager
async def lifespan(app):
    from app.broker import Publisher

    publisher = Publisher()
    app.state.publisher = publisher
    stop = Event()
    worker = None
    if enabled("GENERATOR_ENABLED"):
        worker = Thread(target=generate, args=(publisher.send, stop), daemon=True)
        worker.start()
    try:
        yield
    finally:
        stop.set()
        if worker:
            worker.join(timeout=25)
        publisher.close()


app = FastAPI(title="Movie Service", lifespan=lifespan)


@app.get("/health")
def health(request: Request):
    if request.app.state.publisher.ready():
        return {"status": "ok"}
    raise HTTPException(503, "Kafka недоступна")


@app.post("/events", status_code=201)
def publish(event: MovieEvent, request: Request):
    try:
        request.app.state.publisher.send(event)
    except ValueError as exc:
        raise HTTPException(422, "Событие не соответствует Avro v1") from exc
    except ConnectionError as exc:
        log.exception("publish failed event_id=%s", event.event_id)
        raise HTTPException(503, "Нет подтверждения записи в Kafka") from exc
    return {"event_id": str(event.event_id)}
