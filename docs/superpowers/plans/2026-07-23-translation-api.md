# Translation API Implementation Plan

> **Superseded — do not implement from this plan.** It drove the *original*
> `translation-service` implementation, which used a fire-and-forget publish plus an
> async result on `chat.user.{account}.response.{requestID}` (`natsrouter.RegisterVoid`).
> That transport was later changed to **synchronous NATS request/reply**
> (`natsrouter.Register`; the handler returns `(*TranslateResult, error)` and the reply
> travels on the auto-generated `_INBOX`), the request subject gained a `.text` action
> segment (`chat.user.{account}.request.translate.{siteID}.text`), and the request/result
> models were slimmed to wire-only `{text, targetLang}` / `{translatedText, targetLang}`.
>
> The task-by-task detail this plan originally carried described that async design and
> would mislead a future reader, so it has been removed. For the current, authoritative
> contract see:
>
> - **Design:** [2026-07-23-translation-api-design.md](../specs/2026-07-23-translation-api-design.md)
> - **Client API:** [client-api.md §3.6](../../client-api.md#36-translation-service) — plus the derived [request-reply.md](../../client-api/request-reply.md) view
> - **Code:** `translation-service/` (handler, backends, token auth), `pkg/model/translation.go`, `pkg/subject/subject.go`
