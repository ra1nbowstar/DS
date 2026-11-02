-- Exact grant records so refunds reclaim what was actually credited.
-- Mirrors referral_reward_grants: the credited amount is preserved with six
-- decimals instead of being reconstructed from account_flow text.

CREATE TABLE IF NOT EXISTS rain_point_grants (
 flow_id BIGINT UNSIGNED NOT NULL PRIMARY KEY,
 order_id BIGINT UNSIGNED NOT NULL,
 recipient_id BIGINT UNSIGNED NOT NULL,
 amount DECIMAL(14,6) NOT NULL,
 state VARCHAR(16) NOT NULL DEFAULT 'granted',
 recovered_refund_no VARCHAR(64) NULL,
 recovered_at DATETIME NULL,
 KEY ix_rain_grants_order (order_id, flow_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS team_reward_grants (
 flow_id BIGINT UNSIGNED NOT NULL PRIMARY KEY,
 order_id BIGINT UNSIGNED NOT NULL,
 recipient_id BIGINT UNSIGNED NOT NULL,
 layer TINYINT NULL,
 amount DECIMAL(14,6) NOT NULL,
 state VARCHAR(16) NOT NULL DEFAULT 'granted',
 recovered_refund_no VARCHAR(64) NULL,
 recovered_at DATETIME NULL,
 KEY ix_team_grants_order (order_id, flow_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS member_level_grants (
 order_id BIGINT UNSIGNED NOT NULL PRIMARY KEY,
 order_number VARCHAR(64) NOT NULL,
 user_id BIGINT UNSIGNED NOT NULL,
 level_before TINYINT NOT NULL,
 level_after TINYINT NOT NULL,
 granted_level TINYINT NOT NULL,
 recovered_refund_no VARCHAR(64) NULL,
 recovered_at DATETIME NULL,
 KEY ix_member_level_grants_user (user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;