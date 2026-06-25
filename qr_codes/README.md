# QR codes

Shareable assets for contacting the BT Servant Signal bot **by username link** (not by phone
number). Starting a chat via the link resolves directly to the bot's account identity (ACI), which
avoids the confusing duplicate phone-number-vs-profile thread that first-contact-by-number causes.
See the README "Trust model" section and the gateway docs for background.

## Contact link

- **Username:** `btservant.45`
- **Link:** https://signal.me/#eu/-RGNTp_ER2U74QlijVSyUHxU_EnzcvATLyvcTyCYS8r_jcbr-FlNxJcgZ7fPXTce

Scanning the QR (or opening the link) takes a user straight to a new chat with **BT Servant**.
A one-time Signal "message request" tap is still expected — that's inherent Signal behavior for
any new contact and can't be removed by the bot.

## Files

- `btservant-signal-link.png` — 490×490 PNG, for slides/print.
- `btservant-signal-link.svg` — vector, for crisp scaling.

## Regenerating

The link is tied to the bot's username; if the username is ever deleted/reset, the link changes
and these must be regenerated. Get the current link from the running daemon
(`updateAccount` returns it) and rebuild:

```bash
LINK='https://signal.me/#eu/<current-token>'
uvx segno "$LINK" --scale 10 --border 4 --error m --output qr_codes/btservant-signal-link.png
uvx segno "$LINK" --scale 10 --border 4 --error m --output qr_codes/btservant-signal-link.svg
```
