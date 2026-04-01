-- =============================================================================
-- seed_prod.sql — Полная пересборка продакшн базы из состояния дева
-- =============================================================================
-- Запускать одной командой:
--   psql $DATABASE_URL -f seed_prod.sql
-- =============================================================================

BEGIN;

-- ─── 1. DROP всего (в порядке зависимостей) ──────────────────────────────────

DROP TABLE IF EXISTS log_entries  CASCADE;
DROP TABLE IF EXISTS log          CASCADE;
DROP TABLE IF EXISTS batches      CASCADE;
DROP TABLE IF EXISTS stories      CASCADE;
DROP TABLE IF EXISTS ai_models    CASCADE;
DROP TABLE IF EXISTS ai_platforms CASCADE;
DROP TABLE IF EXISTS targets      CASCADE;
DROP TABLE IF EXISTS schedule     CASCADE;
DROP TABLE IF EXISTS video_urls   CASCADE;
DROP TABLE IF EXISTS settings     CASCADE;

-- ─── 2. CREATE (точная схема дева) ───────────────────────────────────────────

CREATE TABLE settings (
    key   VARCHAR(100) PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE schedule (
    id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    time_utc VARCHAR(5) NOT NULL
);

CREATE TABLE video_urls (
    id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    url        TEXT NOT NULL UNIQUE,
    time_point FLOAT NOT NULL
);

CREATE TABLE ai_platforms (
    id   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(200) NOT NULL,
    url  VARCHAR(500) NOT NULL
);

CREATE TABLE ai_models (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name           VARCHAR(200) NOT NULL,
    url            VARCHAR(200) NOT NULL,
    body           JSONB NOT NULL DEFAULT '{}',
    "order"        INTEGER NOT NULL DEFAULT 0,
    active         BOOLEAN NOT NULL DEFAULT FALSE,
    ai_platform_id UUID REFERENCES ai_platforms(id),
    platform_id    UUID REFERENCES ai_platforms(id),
    type           VARCHAR(50) NOT NULL,
    time_point     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE targets (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name           VARCHAR(200) NOT NULL,
    aspect_ratio_x SMALLINT NOT NULL DEFAULT 9,
    aspect_ratio_y SMALLINT NOT NULL DEFAULT 16,
    active         BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE stories (
    id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    time_point TIMESTAMPTZ NOT NULL DEFAULT now(),
    result     TEXT NOT NULL,
    model_id   UUID REFERENCES ai_models(id)
);

CREATE TABLE batches (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    scheduled_at TIMESTAMPTZ NOT NULL,
    target_id    UUID NOT NULL REFERENCES targets(id),
    status       VARCHAR(30) NOT NULL DEFAULT 'pending',
    story_id     UUID REFERENCES stories(id),
    video_url    TEXT,
    video_file   TEXT,
    data         JSONB,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    CONSTRAINT batches_scheduled_at_target_id_key UNIQUE (scheduled_at, target_id)
);

CREATE TABLE log (
    id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    batch_id   UUID REFERENCES batches(id),
    pipeline   VARCHAR(30) NOT NULL,
    message    TEXT,
    status     VARCHAR(20),
    time_point TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE log_entries (
    id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    log_id     UUID NOT NULL REFERENCES log(id),
    message    TEXT NOT NULL,
    level      VARCHAR(10) NOT NULL DEFAULT 'info',
    time_point TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ─── 3. INSERT конфигурационных данных ───────────────────────────────────────

-- settings
INSERT INTO settings (key, value) VALUES ('batch_lifetime', '60');
INSERT INTO settings (key, value) VALUES ('buffer_hours', '24');
INSERT INTO settings (key, value) VALUES ('emulation_mode', '0');
INSERT INTO settings (key, value) VALUES ('file_lifetime', '10');
INSERT INTO settings (key, value) VALUES ('log_lifetime', '10');
INSERT INTO settings (key, value) VALUES ('loop_interval', '15');
INSERT INTO settings (key, value) VALUES ('metaprompt', 'Для публикации в соцсетях в формате Shorts, Reels. Залипательное, хайповое. Сюжет должен генерироваться по описанию: неожиданный, вплоть до абсурдного, удивляющий, умеренно шокирующий, при этом красивый. Не реклама, просто интересный сюжет.

Вероятность присутствия людей 50%.
Вероятность фантастического, кибер-реалистичного 25%.
Вероятность инопланетного 25%.
Вероятность юмористического 60%.');
INSERT INTO settings (key, value) VALUES ('notify_email', '');
INSERT INTO settings (key, value) VALUES ('notify_phone', '');
INSERT INTO settings (key, value) VALUES ('short_log_lifetime', '30');
INSERT INTO settings (key, value) VALUES ('system_prompt', 'Ты генерируешь только один короткий сценарий для вертикального видео (Shorts/Reels). Никаких пояснений, вопросов и заголовков. Первый ответ — только чистый текст сценария. Сценарий должен легко укладываться в заданную продолжительность. Пиши очень коротко, не перегружай деталями. Не делай длинных сценариев, они не умещаются в продолжительность. Скорость событий должна быть в три раза меньше, чем тебе хочется.');
INSERT INTO settings (key, value) VALUES ('video_duration', '8');
INSERT INTO settings (key, value) VALUES ('vk_publish_story', '1');
INSERT INTO settings (key, value) VALUES ('vk_publish_wall', '1');

-- ai_platforms
INSERT INTO ai_platforms (id, name, url) VALUES ('0c8d1e1c-fe65-45d3-a1c3-be69e7941e17', 'fal', 'https://queue.fal.run/fal-ai');
INSERT INTO ai_platforms (id, name, url) VALUES ('1b696238-1fdd-4b95-bff0-86a37be13c78', 'OpenRouter', 'https://openrouter.ai/api/v1/chat/completions');

-- ai_models
INSERT INTO ai_models (id, name, url, body, "order", active, ai_platform_id, platform_id, type) VALUES ('8c23daf4-1a70-4e13-ada3-4f7d0f1822ef', 'sora-2', 'sora-2/text-to-video', '{"prompt": "{}", "duration": "{int}", "aspect_ratio": "{:d}:{:d}"}', 1, TRUE, '0c8d1e1c-fe65-45d3-a1c3-be69e7941e17', '0c8d1e1c-fe65-45d3-a1c3-be69e7941e17', 'text-to-video');
INSERT INTO ai_models (id, name, url, body, "order", active, ai_platform_id, platform_id, type) VALUES ('f0293755-8bf2-4654-8d7c-eb08dbd919c6', 'veo2', 'veo2', '{"prompt": "{}", "duration": "{:d}s", "aspect_ratio": "{:d}:{:d}"}', 2, FALSE, '0c8d1e1c-fe65-45d3-a1c3-be69e7941e17', '0c8d1e1c-fe65-45d3-a1c3-be69e7941e17', 'text-to-video');
INSERT INTO ai_models (id, name, url, body, "order", active, ai_platform_id, platform_id, type) VALUES ('2b0ef68a-52e8-4d37-8b29-716c4fb7d0dc', 'minimax/video-01', 'minimax/video-01', '{"prompt": "{}", "duration": "{:d}s", "aspect_ratio": "{:d}:{:d}"}', 3, FALSE, '0c8d1e1c-fe65-45d3-a1c3-be69e7941e17', '0c8d1e1c-fe65-45d3-a1c3-be69e7941e17', 'text-to-video');
INSERT INTO ai_models (id, name, url, body, "order", active, ai_platform_id, platform_id, type) VALUES ('31d3c708-acd9-4a90-9efb-909219814b30', 'kling-video/v1.6/standard', 'kling-video/v1.6/standard/text-to-video', '{"prompt": "{}", "duration": "{:d}", "aspect_ratio": "{:d}:{:d}"}', 4, FALSE, '0c8d1e1c-fe65-45d3-a1c3-be69e7941e17', '0c8d1e1c-fe65-45d3-a1c3-be69e7941e17', 'text-to-video');
INSERT INTO ai_models (id, name, url, body, "order", active, ai_platform_id, platform_id, type) VALUES ('c2bbc74a-df93-4388-935a-36c96e49e805', 'qwen3.6-plus-preview', 'qwen/qwen3.6-plus-preview:free', '{"messages": [{"role": "system", "content": "{}"}, {"role": "user", "content": "{}"}], "max_tokens": 300, "temperature": 0.9}', 1, TRUE, '1b696238-1fdd-4b95-bff0-86a37be13c78', '1b696238-1fdd-4b95-bff0-86a37be13c78', 'text');
INSERT INTO ai_models (id, name, url, body, "order", active, ai_platform_id, platform_id, type) VALUES ('f692a8c7-e6c0-4ddf-a52d-8977f10b7e9c', 'llama-3.1-8b-instruct', 'meta-llama/llama-3.1-8b-instruct:free', '{"messages": [{"role": "system", "content": "{}"}, {"role": "user", "content": "{}"}], "max_tokens": 300, "temperature": 0.9}', 2, FALSE, '1b696238-1fdd-4b95-bff0-86a37be13c78', '1b696238-1fdd-4b95-bff0-86a37be13c78', 'text');
INSERT INTO ai_models (id, name, url, body, "order", active, ai_platform_id, platform_id, type) VALUES ('6345fd09-349f-4bcf-9b07-37f20fe6bed3', 'mistral-7b-instruct', 'mistralai/mistral-7b-instruct:free', '{"messages": [{"role": "system", "content": "{}"}, {"role": "user", "content": "{}"}], "max_tokens": 300, "temperature": 0.9}', 3, FALSE, '1b696238-1fdd-4b95-bff0-86a37be13c78', '1b696238-1fdd-4b95-bff0-86a37be13c78', 'text');

-- targets
INSERT INTO targets (id, name, aspect_ratio_x, aspect_ratio_y, active) VALUES ('691d67b7-ff29-48cf-af77-0bff68986fa2', 'Дзен', 16, 9, FALSE);
INSERT INTO targets (id, name, aspect_ratio_x, aspect_ratio_y, active) VALUES ('b62dde43-69fa-4a82-89e7-52ed67654703', 'VKontakte', 9, 16, TRUE);

-- video_urls (история уже отправленных URL — защита от повторной публикации)
INSERT INTO video_urls (id, url, time_point) VALUES ('05ec945d-5698-405e-819a-8aea3a1c872a', 'https://v3b.fal.media/files/b/0a944c08/_LYa9qqL4x8d3AX7s1ZY__F2lo76GO.mp4', 1774909548.810932);
INSERT INTO video_urls (id, url, time_point) VALUES ('27de93ca-4f4b-4b9d-9b06-f5f1cfa32585', 'https://v3b.fal.media/files/b/0a946068/wobUg4oVK537Wf_rR1b7k_AUqhMGGU.mp4', 1774961717.7855332);
INSERT INTO video_urls (id, url, time_point) VALUES ('2ae6a53c-64ae-470c-9f1e-ffdbcc078d98', 'https://v3b.fal.media/files/b/0a945640/o8tbTU3W5qSGvnyVZ0oop_H5EQUuSa.mp4', 1774935716.4495294);
INSERT INTO video_urls (id, url, time_point) VALUES ('4015ce8e-d3f0-4a54-9798-d4bd7257159e', 'https://v3b.fal.media/files/b/0a945f74/3FQmoMfkWDj-SqkcVrP5l_ZcnrwxJM.mp4', 1774959245.2138338);
INSERT INTO video_urls (id, url, time_point) VALUES ('5816ac7f-08b3-4fa2-8004-9cd5f7d039f5', 'https://v3b.fal.media/files/b/0a945f68/WxxOKIWzeeMADbAcQSVgK_iZNSadiM.mp4', 1774959157.1172357);
INSERT INTO video_urls (id, url, time_point) VALUES ('5936df2b-8830-4582-9bf9-12ad3184af18', 'https://v3b.fal.media/files/b/0a9465dc/Q7cra-NLX4CRIeYetMT7p_zfHmjWCP.mp4', 1774975669.3338249);
INSERT INTO video_urls (id, url, time_point) VALUES ('6907c692-802d-4be7-a775-5f6b0f2324bb', 'https://v3b.fal.media/files/b/0a9447c2/DUXph_L7eUO8r117gnCfi_output.mp4', 1774898614.0067003);
INSERT INTO video_urls (id, url, time_point) VALUES ('6ea1b3ff-3014-4319-b3a1-596a9ed41a4f', 'https://v3b.fal.media/files/b/0a946542/jlCFS0YLJssIuO0Pw7Wla_94bOp3Qj.mp4', 1774974138.1890209);
INSERT INTO video_urls (id, url, time_point) VALUES ('70921e9f-9f21-4166-8c2f-0acb7489da1b', 'https://v3b.fal.media/files/b/0a946159/cGxeI-BFDtXfqfHq9SQlC_AB13TRry.mp4', 1774964127.9396536);
INSERT INTO video_urls (id, url, time_point) VALUES ('7765ebe1-34e2-4e62-a521-96939a384370', 'https://v3b.fal.media/files/b/0a946497/mhChheRh6a1_62fz-MhLV_iyPwZFGs.mp4', 1774972418.073316);
INSERT INTO video_urls (id, url, time_point) VALUES ('821e3648-a164-4e34-b384-0d7f8f281906', 'https://v3b.fal.media/files/b/0a945efd/gPMDTgS0NJvKXcoZ4OnUh_output.mp4', 1774958088.4042878);
INSERT INTO video_urls (id, url, time_point) VALUES ('8fd6d0e4-7192-4d48-a274-4de7c306f4b1', 'https://v3b.fal.media/files/b/0a944bd3/q3VGyC2NNATyx-wI5cwln_wD1FDL2q.mp4', 1774909025.4732525);
INSERT INTO video_urls (id, url, time_point) VALUES ('b333409c-031b-48cd-a413-f22c87b86877', 'https://v3b.fal.media/files/b/0a946839/tztesyGaciiGNQ4hxjfj8_Bv7vZxNM.mp4', 1774981725.7229888);
INSERT INTO video_urls (id, url, time_point) VALUES ('ba7e68c5-f8b8-4146-a3a0-0246810729ab', 'https://v3b.fal.media/files/b/0a9446a9/Dz_2DIIIr5bdUdarwnSBw_output.mp4', 1774895804.8400376);
INSERT INTO video_urls (id, url, time_point) VALUES ('ce536980-4633-448e-aa75-8ba4171200f3', 'https://v3b.fal.media/files/b/0a945ff4/l2VzO8ZuYGqN-QknC9tSh_pvqzpE8x.mp4', 1774960553.631015);
INSERT INTO video_urls (id, url, time_point) VALUES ('ee615da4-3242-4a96-b8af-570c062fb300', 'https://v3b.fal.media/files/b/0a94686f/e1lj8zYpoCdhUStnU_SpF_24GPLAiS.mp4', 1774982262.3212755);

-- schedule оставляем пустым — приложение добавит слот 03:00 при первом запуске,
-- после чего можно настроить расписание через UI.

COMMIT;
