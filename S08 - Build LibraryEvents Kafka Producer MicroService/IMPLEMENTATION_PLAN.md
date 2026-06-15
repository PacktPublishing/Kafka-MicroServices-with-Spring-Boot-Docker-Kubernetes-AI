# Implementation Plan
## Library Events Producer API v2

- **Reference:** `docs/PRD.md`
- **Tech Stack:** Spring Boot 4, Java 25, Gradle, Kafka
- **Configuration Style:** `application.yml` (primary)

## 1) Delivery Strategy
Build the application layer by layer so each step is focused on feature delivery and releasable. Keep controller thin, business rules centralized in service, and Kafka concerns isolated in producer/config layers.

> **Testing note:** Automated unit and integration tests are intentionally deferred to a dedicated testing section in the course. Until then, the implementation plan focuses on code structure, validation rules, and runtime behavior.

## 2) Layered Plan and Milestones

### Layer 0 - Project Foundation
**Goal:** Prepare dependencies, package layout, and YAML-based configuration.

**Tasks**
- Validate/update `build.gradle` dependencies:
  - `spring-boot-starter-web`
  - `spring-kafka`
  - `spring-boot-starter-actuator`
  - `org.springframework.boot:spring-boot-starter-validation`
  - `com.fasterxml.jackson.core:jackson-databind:2.20.2` (Kafka `JsonSerializer` runtime compatibility)
  - Ensure Jackson alignment note is documented in code comments/PR: this version must align with `jackson-annotations:2.20` constrained by the Jackson 3.x BOM.
  - test dependencies including `spring-kafka-test`
- Keep one config source: `src/main/resources/application.yml`.
- Create package structure under `com.learnkafka`:
  - `controller`, `domain`, `service`, `producer`, `config`, `exception`
- Add app constants for API base path and defaults where useful.

**Deliverables**
- App boots with no custom logic errors.
- `application.yml` in place and loaded.

**Exit Criteria**
- `./gradlew test` passes with baseline context test.

---

### Layer 1 - Domain Model + Validation
**Goal:** Implement core request/event models and validation constraints.

**Tasks**
- Create `Book` model:
  - `bookId` (required)
  - `bookName` (required, non-blank)
  - `bookAuthor` (required, non-blank)
- Create `LibraryEvent` model:
  - `libraryEventId`
  - `eventType` (`ADD`, `UPDATE`)
  - `book` (required)
- Create enum `LibraryEventType` with values `ADD`, `UPDATE`.
- Add Jakarta Bean Validation annotations.

**Deliverables**
- Domain classes with validation metadata.

**Exit Criteria**
- `Book`, `LibraryEvent`, and `LibraryEventType` are implemented and compile with the required Jakarta Bean Validation metadata.

---

### Layer 2 (Flow 1) - POST Endpoint + Kafka Producer
**Goal:** Deliver the complete POST flow end-to-end: controller → service → Kafka producer.

**Tasks**

*Controller*
- Add `POST /v1/libraryevent` handler in `LibraryEventsController`.
- Validate request body using `@Valid`.
- Enforce `eventType == ADD`; reject with `400` otherwise.
- Return `201 Created` with the full `LibraryEvent` payload.

*Service*
- Implement `LibraryEventService.createLibraryEvent(LibraryEvent event)`.
- Enforce rule: incoming event must have `eventType == ADD`.
- Delegate publish to `LibraryEventProducer`.

*Kafka Producer*
- Create `LibraryEventProducer` using `KafkaTemplate<Long, LibraryEvent>`.
- Read topic from YAML (`app.kafka.topic`).
- Publish with key = `libraryEventId`.
- Add publish result logging (success/failure).
- Wrap/propagate failures as domain-specific exception.

**Deliverables**
- POST handler wired through service to a working Kafka producer.

**Exit Criteria**
- A valid `ADD` event posted to the endpoint is published to the Kafka topic and returns `201` with full payload.

---

### Layer 3 (Flow 2) - PUT Endpoint + Kafka Producer
**Goal:** Deliver the complete PUT flow end-to-end: controller → service → Kafka producer (extending Flow 1).

**Tasks**

*Controller*
- Add `PUT /v1/libraryevent` handler in `LibraryEventsController`.
- Validate request body using `@Valid`.
- Enforce `eventType == UPDATE`; reject with `400` otherwise.
- Enforce non-null `libraryEventId`; reject with `400` if missing.
- Return `200 OK` with the full `LibraryEvent` payload.

*Service*
- Implement `LibraryEventService.updateLibraryEvent(LibraryEvent event)`.
- Delegate publish to `LibraryEventProducer`.

*Kafka Producer*
- Extend `LibraryEventProducer` to handle the UPDATE publish path if needed.
- Reuse topic config and key strategy from Flow 1.

**Deliverables**
- PUT handler wired through service to the Kafka producer.

**Exit Criteria**
- A valid `UPDATE` event with a non-null id PUT to the endpoint is published to the Kafka topic and returns `200` with full payload.

---

### Layer 4 - Exception Handling + Error Contract
**Goal:** Standardize API error responses as per PRD.

**Tasks**
- Add custom exceptions:
  - `InvalidLibraryEventException`
  - `LibraryEventPublishException`
- Implement `@RestControllerAdvice`:
  - `400` for validation/business mismatch errors
  - `500` for publish/internal errors
- Use common error response model:
  - timestamp, status, message, path, optional traceId

**Deliverables**
- Global exception handler and error response schema.

**Exit Criteria**
- Validation, business, and publish failures are translated to the documented error response format.

---

### Layer 5 - Configuration + Observability (YAML)
**Goal:** Externalize runtime settings and basic operational visibility.

**Tasks**
- Configure in `application.yml`:
  - Kafka bootstrap servers
  - producer retries/backoff
  - topic name (`app.kafka.topic`)
  - actuator endpoint exposure
- Add metrics/logging for request and publish outcomes.
- Keep logs structured with `libraryEventId`, `eventType`, `bookId`.

**Deliverables**
- Runtime-ready YAML config and health/metrics visibility.

**Exit Criteria**
- Health endpoint is exposed and required runtime settings are externalized in YAML.

---

### Layer 6 - Documentation + Release Readiness
**Goal:** Keep design and usage discoverable and release-safe.

**Tasks**
- Add/update API docs (OpenAPI optional in this phase).
- Add request/response examples and error examples.
- Document local run/test instructions in `README.md`.
- Final review against `docs/PRD.md` acceptance criteria.

**Deliverables**
- Updated docs and release checklist.

**Exit Criteria**
- Team can run, test, and validate behavior locally with minimal setup.

## 3) Dedicated Testing Section (Separate Course Section)

### Unit Testing
**Goal:** Validate component behavior in isolation after the implementation layers are complete.

**Planned Scope**
- Domain validation coverage for `Book`, `LibraryEvent`, and `LibraryEventType` usage.
- Controller tests for request validation, status codes, and `eventType` enforcement.
- Service tests for business-rule enforcement and producer delegation.
- Producer tests for topic/key/payload usage and failure handling.
- Exception handler tests for standardized `400` and `500` responses.

### Integration Testing
**Goal:** Validate end-to-end REST-to-Kafka behavior in the dedicated testing section.

**Planned Scope**
- Add integration tests using embedded Kafka or Testcontainers.
- Verify POST (`ADD`) publishes one message and returns full payload.
- Verify PUT (`UPDATE`) publishes one message and returns full payload.
- Add negative coverage for:
  - wrong event type by method
  - missing `libraryEventId` on PUT
  - missing `book`/book fields

**Exit Criteria**
- Dedicated testing section covers the PRD testing requirements with passing unit and integration suites.

## 4) Recommended File-Level Implementation Order
1. `build.gradle`
2. `src/main/resources/application.yml`
3. `src/main/java/com/learnjava/domain/Book.java`
4. `src/main/java/com/learnjava/domain/LibraryEvent.java`
5. `src/main/java/com/learnjava/domain/LibraryEventType.java`
6. `src/main/java/com/learnjava/controller/LibraryEventsController.java` — POST handler
7. `src/main/java/com/learnjava/service/LibraryEventService.java` — `createLibraryEvent`
8. `src/main/java/com/learnjava/producer/LibraryEventProducer.java` — initial producer (POST path)
9. `src/main/java/com/learnjava/controller/LibraryEventsController.java` — PUT handler
10. `src/main/java/com/learnjava/service/LibraryEventService.java` — `updateLibraryEvent`
11. `src/main/java/com/learnjava/producer/LibraryEventProducer.java` — extend producer (PUT path)
12. `src/main/java/com/learnjava/exception/*`
13. `src/main/java/com/learnjava/config/*` (Kafka/topic properties)
14. `src/test/java/com/learnjava/*` unit + integration tests (covered in the dedicated testing section)

## 5) Acceptance Checklist (Mapped to PRD)
- [ ] POST endpoint exists and enforces `eventType=ADD`.
- [ ] PUT endpoint exists and enforces `eventType=UPDATE`.
- [ ] PUT rejects null/missing `libraryEventId`.
- [ ] Book and mandatory fields are validated.
- [ ] Success responses return full event payload.
- [ ] Valid events are published to Kafka topic from YAML config.
- [ ] `400` and `500` error responses follow standardized shape.
- [ ] Unit and integration tests are implemented in the dedicated testing section.

## 6) Suggested `application.yml` Baseline
```yaml
server:
  port: 8080

spring:
  application:
    name: library-events-producer-v2
  kafka:
    bootstrap-servers: localhost:9092
    producer:
      acks: all
      retries: 10
      key-serializer: org.apache.kafka.common.serialization.LongSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
      properties:
        retry:
          backoff:
            ms: 1000

management:
  endpoints:
    web:
      exposure:
        include: health,info,metrics
  endpoint:
    health:
      probes:
        enabled: true

app:
  kafka:
    topic: library-events
```

## 7) Commit Plan
1. `chore: add dependencies and yaml config baseline`
2. `feat: add domain model and validation rules`
3. `feat(flow1): add POST endpoint, service, and kafka producer`
4. `feat(flow2): add PUT endpoint, service, and extend kafka producer`
5. `feat: add exception handling and error contract`
6. `test: add unit tests for controller and service` *(dedicated testing section)*
7. `test: add integration tests for kafka publish flow` *(dedicated testing section)*
8. `docs: align readme and api examples with prd`


