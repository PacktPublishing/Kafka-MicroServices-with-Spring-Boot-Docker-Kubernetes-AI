# Product Requirements Document (PRD)
## Library Events Producer API v2

- **Version:** 1.1 (Draft)
- **Date:** 2026-03-21
- **Project:** `library-events-producer-v2`
- **Tech Stack:** Spring Boot 4, Java 25, Gradle, Apache Kafka

## 1. Purpose
Build a REST API application that exposes `POST` and `PUT` endpoints for library events and publishes each valid request to a Kafka topic.

The service acts as an event producer for downstream systems that consume book lifecycle updates.

## 2. Background
The platform needs an event-driven integration point for library data changes. Upstream applications will call this API to emit standardized events representing new books and book updates.

## 3. Goals
- Expose REST endpoints for publishing library events.
- Enforce event semantics by HTTP method:
  - `POST` -> `ADD`
  - `PUT` -> `UPDATE`
- Validate request payloads before publishing.
- Publish to Kafka using a consistent JSON event contract.
- Return full event payload in successful responses.

## 4. Non-Goals
- No consumer-side processing in this service.
- No database persistence for events in this phase.
- No authentication/authorization in this phase.
- No dead-letter-topic flow in this phase.

## 5. Stakeholders
- **Upstream Application Teams:** produce events through REST calls.
- **Consumer Application Teams:** consume Kafka events.
- **Platform/Infra Team:** Kafka operations and runtime standards.

## 6. Scope
### In Scope
- REST API with `POST` and `PUT` endpoints.
- Payload validation and error handling.
- Kafka publishing for valid events.
- Basic observability (logs/metrics/health).

### Out of Scope
- Event replay UI/tooling.
- DLQ/DLT routing.
- Security integration (authN/authZ).

## 7. Functional Requirements

### FR-1: Create Event API (`POST`)
- Endpoint accepts `LibraryEvent` payload.
- Service **strictly forces** event type to `ADD`.
  - If client sends a different `eventType`, request is rejected.
- Valid payload is published to Kafka.
- Success response returns full published payload.

### FR-2: Update Event API (`PUT`)
- Endpoint accepts `LibraryEvent` payload.
- Service **strictly forces** event type to `UPDATE`.
  - If client sends a different `eventType`, request is rejected.
- `libraryEventId` is required.
- Valid payload is published to Kafka.
- Success response returns full published payload.

### FR-3: Validation Rules
- `eventType` allowed values: `ADD`, `UPDATE`.
- `book` object is mandatory for both endpoints.
- Book fields are mandatory:
  - `bookId`
  - `bookName`
  - `bookAuthor`
- `libraryEventId` behavior:
  - For `ADD`: supplied by upstream application.
  - For `UPDATE`: required.

### FR-4: Error Handling
- Return `400 Bad Request` for validation failures and method/eventType mismatches.
- Return `500 Internal Server Error` for unexpected Kafka publish failures.
- Error response includes a clear message and timestamp (and trace/correlation id when available).

### FR-5: Kafka Publish Behavior
- Publish each valid event to configured Kafka topic.
- Message payload uses JSON representation of `LibraryEvent`.
- Use `libraryEventId` as key when available.
- Log publish success/failure with key identifiers (`libraryEventId`, `eventType`, `bookId`).

## 8. API Contract (Draft)

### 8.1 POST `/v1/library-events`
Publishes an `ADD` event.

**Request Example**
```json
{
  "libraryEventId": 1001,
  "eventType": "ADD",
  "book": {
    "bookId": 123,
    "bookName": "Kafka for Developers",
    "bookAuthor": "Jane Doe"
  }
}
```

**Success Response**
- `201 Created`
- Body: full `LibraryEvent` payload

**Failure Responses**
- `400 Bad Request` (invalid payload or non-ADD eventType)
- `500 Internal Server Error` (Kafka/internal failure)

### 8.2 PUT `/v1/library-events`
Publishes an `UPDATE` event.

**Request Example**
```json
{
  "libraryEventId": 1001,
  "eventType": "UPDATE",
  "book": {
    "bookId": 123,
    "bookName": "Kafka for Developers - 2nd Edition",
    "bookAuthor": "Jane Doe"
  }
}
```

**Success Response**
- `200 OK`
- Body: full `LibraryEvent` payload

**Failure Responses**
- `400 Bad Request` (invalid payload, missing id, or non-UPDATE eventType)
- `500 Internal Server Error` (Kafka/internal failure)

## 9. Data Model

### LibraryEvent
- `libraryEventId`: Long
- `eventType`: Enum (`ADD`, `UPDATE`)
- `book`: Book

### Book
- `bookId`: Long
- `bookName`: String
- `bookAuthor`: String

## 10. Kafka Requirements
- **Topic:** configurable (default `library-events`).
- **Serialization:** JSON for message value.
- **Keying:** `libraryEventId`.
- **Delivery Semantics:** at-least-once.
- **Producer Reliability:** configurable retries and backoff.

## 11. Non-Functional Requirements
- **Performance:** p95 API response latency target under normal broker conditions (target: <300ms, to be validated).
- **Reliability:** robust handling of transient Kafka failures with retry configuration.
- **Scalability:** stateless service, horizontally scalable.
- **Observability:**
  - Structured logs with event identifiers.
  - Metrics for request counts, publish success/failure, latency.
  - Health endpoints for liveness/readiness.
- **Security:** TLS in transit as platform standard; no app-level auth required in this phase.

## 12. Assumptions
- Kafka cluster and topic provisioning are handled externally.
- Upstream app provides valid `libraryEventId` for new (`ADD`) events.
- Consumers understand and accept this JSON schema.

## 13. Risks and Mitigations
- **Risk:** Event contract drift between producer and consumers.
  - **Mitigation:** versioned schema and contract tests.
- **Risk:** Duplicate delivery due to retries.
  - **Mitigation:** document at-least-once semantics; consumer idempotency.
- **Risk:** Kafka outages reduce API success rates.
  - **Mitigation:** retries, clear error handling, operational alerts.

## 14. Acceptance Criteria
- `POST /v1/library-events` with valid `ADD` payload publishes one Kafka message and returns full payload.
- `PUT /v1/library-events` with valid `UPDATE` payload publishes one Kafka message and returns full payload.
- `POST` rejects payloads where `eventType != ADD`.
- `PUT` rejects payloads where `eventType != UPDATE`.
- `PUT` rejects missing/null `libraryEventId`.
- Invalid payloads return `400` with actionable validation details.
- Kafka failures return `500` with non-sensitive error message.
- Unit and integration tests cover success and error paths.

## 15. Test Requirements
- **Unit Tests:** validation rules, endpoint logic, eventType enforcement.
- **Integration Tests:** REST-to-Kafka publishing flow.
- **Negative Tests:**
  - missing `book`
  - missing book fields
  - wrong `eventType` by method
  - missing `libraryEventId` on `PUT`

## 16. Resolved Decisions (from Open Questions)
- POST must strictly enforce `eventType=ADD`: **Yes**.
- `libraryEventId` for `ADD` generated by API or client: **Provided by upstream app**.
- Success response shape: **Full payload**.
- API authentication in this phase: **Not required**.
- Dead-letter topic behavior in this phase: **Not required now**.

