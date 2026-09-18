# RPS Arena Online — Complete Cloudflare Worker

این پروژه یک بازی RPS Arena را به‌صورت Static Assets + Worker + Durable Object روی Cloudflare اجرا می‌کند.

ساختار:
- `worker.js` سرور و WebSocket و Roomها
- `wrangler.toml` تنظیمات Workers Assets و Durable Object SQLite
- `public/index.html` خود بازی RPS Arena + صفحه بازی آنلاین

در Cloudflare Workers Builds:
- Build command: خالی
- Deploy command: `npx wrangler deploy`

بعد از Deploy، باز کردن ریشه Worker باید خود بازی را نشان دهد. مسیر `/room/CODE` برای WebSocket رزرو شده است.
