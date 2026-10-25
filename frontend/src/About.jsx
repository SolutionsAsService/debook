import { ArrowRight, Boxes, Database, ExternalLink, Link2, Radio, ShieldCheck } from 'lucide-react'

const isRussian = window.location.pathname === '/ru' || window.location.pathname.startsWith('/ru/')
const text = (ru, en) => isRussian ? ru : en
const repo = 'https://github.com/ThothDivision/debook'

const pipeline = [
  { icon: Radio, title: 'API', detail: text('Go HTTP · идемпотентность · rate limit', 'Go HTTP · idempotency · rate limiting') },
  { icon: Database, title: 'PostgreSQL', detail: text('Блокировка слота и outbox в одной транзакции', 'Slot lock and outbox in one transaction') },
  { icon: Boxes, title: 'Kafka', detail: text('Outbox relay → workflow / Saga → ClickHouse', 'Outbox relay → workflow / Saga → ClickHouse') },
  { icon: Link2, title: 'Autheo', detail: text('Пакетная запись доказательств владения', 'Batched on-chain ownership proofs') },
]

export default function About({ demo }) {
  return <section className="about panel" id="about">
    <div className="about-head">
      <div>
        <div className="eyebrow"><ShieldCheck size={13} /> DEBOOK</div>
        <h1>{text('Высоконагруженный сервис бронирования с on-chain доказательствами', 'High-load booking with on-chain proofs')}</h1>
        <p className="about-lead">{text(
          'Debook находит и бронирует временные слоты, выдерживая пики трафика, и записывает доказательство владения бронью в блокчейн Autheo. Подходит для записи к врачу, аренды переговорных, окон доставки, билетов на события и любых других ограниченных по времени ресурсов.',
          'Debook discovers and reserves time slots under heavy traffic, then records proof of booking ownership on the Autheo blockchain. Use it for appointments, meeting rooms, delivery windows, timed tickets and any other limited, time-based resource.')}</p>
      </div>
      <div className="about-links">
        <a className="about-cta" href="#lab">{text('Открыть Load Lab', 'Open the Load Lab')} <ArrowRight size={14} /></a>
        <a className="about-link" href={repo} target="_blank" rel="noreferrer">GitHub <ExternalLink size={12} /></a>
        <a className="about-link" href={`${repo}/tree/main/docs/en`} target="_blank" rel="noreferrer">{text('Документация', 'Docs')} <ExternalLink size={12} /></a>
      </div>
    </div>
    <ol className="pipeline">{pipeline.map(({ icon: Icon, title, detail }) => <li key={title}><Icon size={16} /><b>{title}</b><span>{detail}</span></li>)}</ol>
    <div className="about-steps">
      <div><b>1 · {text('Получить слоты', 'Fetch slots')}</b><span><code>GET /api/v1/slots</code></span></div>
      <div><b>2 · {text('Забронировать слот', 'Book a slot')}</b><span><code>POST /api/v1/bookings</code> + Idempotency-Key</span></div>
      <div><b>3 · {text('Проверить владельца', 'Check the owner')}</b><span><code>GET /api/v1/bookings/:id/ownership</code></span></div>
    </div>
    <p className="about-mode">{demo
      ? text('Сейчас включён демо-режим: запросы обрабатывает симулятор API прямо в браузере, поэтому тест можно запустить без бэкенда. Чтобы нагрузить настоящий сервис, укажите его адрес в «Параметры API».', 'Demo mode is on: requests are served by an API simulator running in your browser, so you can run a test without a backend. To load-test a real deployment, enter its URL under “API parameters”.')
      : text('Тест отправляет запросы на указанный API. Введите demo в «Параметры API», чтобы использовать встроенный симулятор.', 'Tests send requests to the configured API. Enter demo under “API parameters” to use the built-in simulator instead.')}</p>
  </section>
}
