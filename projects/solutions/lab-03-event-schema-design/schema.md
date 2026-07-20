# StreamFlow Event Schema

This document is the contract between event producers and consumers. Producers
must create events that follow these rules; consumers may rely on these fields
and types being present after validation.

## Common Event Fields

| Field | Type | Required | Example | Rule |
| ----- | ---- | -------- | ------- | ---- |
| `event_id` | string | yes | `evt_001` | Must be unique and non-empty |
| `event_type` | string | yes | `page_view` | Must be an allowed event type |
| `user_id` | string | yes | `user_123` | Must be non-empty |
| `event_ts` | string | yes | `2026-06-30T14:30:00Z` | Must be an ISO 8601 timestamp with a timezone |
| `source` | string | yes | `web` | Must be an allowed source |
| `payload` | object | yes | `{"page":"/home"}` | Must contain details for the selected event type |

Allowed event types are `page_view`, `video_play`, `video_pause`,
`add_to_cart`, and `purchase`.

Allowed sources are `web`, `mobile`, `api`, and `system`.

## Event-Specific Payloads

Common metadata stays at the top level so every consumer can locate it in the
same place. Details that only apply to one kind of event belong in `payload`.

| Event type | Payload fields | Rules |
| --- | --- | --- |
| `page_view` | `page` | Non-empty string |
| `video_play` | `video_id`, `position_seconds` | Non-empty video ID and a non-negative number |
| `video_pause` | `video_id`, `position_seconds` | Non-empty video ID and a non-negative number |
| `add_to_cart` | `sku`, `quantity` | Non-empty SKU and a positive integer quantity |
| `purchase` | `amount`; optional `plan` | Positive numeric amount; when present, plan is a non-empty string |

## Example

```json
{
  "event_id": "evt_003",
  "event_type": "purchase",
  "user_id": "user_103",
  "event_ts": "2026-06-30T14:02:00Z",
  "source": "web",
  "payload": {
    "plan": "pro",
    "amount": 19.99
  }
}
```

The JSON Lines data files store this object on one line because each line must
be independently readable as one event.
