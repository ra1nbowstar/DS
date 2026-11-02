"""退款回收：雨点、团队奖励、会员等级的 grant 记账与按比例回冲。

集成分支已有 referral_reward_grants / revoke_referral_rewards 与
order_member_reward_refunds 负责推荐奖励和会员奖励积分；本文件验证本次
新增的雨点、团队奖励和会员等级三条链路。
"""
from contextlib import contextmanager
from decimal import Decimal
import uuid

import pytest


@pytest.fixture
def refund_db(monkeypatch):
    from scripts.security_isolation.safety import guard
    guard()

    import pymysql
    from core.config import get_db_config
    from services import finance_service

    @contextmanager
    def connect():
        conn = pymysql.connect(**get_db_config(), cursorclass=pymysql.cursors.DictCursor)
        try:
            yield conn
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    monkeypatch.setattr(finance_service, 'get_conn', connect)
    state = {'users': [], 'orders': []}
    yield connect, state

    with connect() as conn:
        with conn.cursor() as cur:
            for order_no in state['orders']:
                cur.execute('DELETE FROM finance_events WHERE order_number=%s', (order_no,))
                cur.execute(
                    'DELETE g FROM rain_point_grants g JOIN orders o ON o.id=g.order_id '
                    'WHERE o.order_number=%s', (order_no,))
                cur.execute(
                    'DELETE g FROM team_reward_grants g JOIN orders o ON o.id=g.order_id '
                    'WHERE o.order_number=%s', (order_no,))
                cur.execute('DELETE FROM member_level_grants WHERE order_number=%s', (order_no,))
            for user_id in state['users']:
                cur.execute('DELETE FROM coupons WHERE user_id=%s', (user_id,))
                cur.execute('DELETE FROM account_flow WHERE related_user=%s', (user_id,))
                cur.execute('DELETE FROM points_log WHERE user_id=%s', (user_id,))
            for order_no in state['orders']:
                cur.execute('DELETE FROM orders WHERE order_number=%s', (order_no,))
            for user_id in state['users']:
                cur.execute('DELETE FROM users WHERE id=%s', (user_id,))
            conn.commit()


def _user(connect, state, **fields):
    columns = ['name'] + list(fields)
    values = ['u_' + uuid.uuid4().hex[:10]] + list(fields.values())
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                'INSERT INTO users(%s) VALUES(%s)' % (','.join(columns), ','.join(['%s'] * len(columns))),
                tuple(values),
            )
            user_id = cur.lastrowid
        conn.commit()
    state['users'].append(user_id)
    return user_id


def _order(connect, state, order_number, user_id, total=100, status='refunding'):
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                'INSERT INTO orders(order_number,user_id,merchant_id,total_amount,status) '
                "VALUES(%s,%s,%s,%s,%s)",
                (order_number, user_id, user_id, total, status),
            )
            order_id = cur.lastrowid
        conn.commit()
    state['orders'].append(order_number)
    return order_id


def _value(connect, user_id, field):
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT %s AS value FROM users WHERE id=%%s' % field, (user_id,))
            return Decimal(str(cur.fetchone()['value'] or 0))


def _refund(connect, order_number, event_key, amount):
    from services.finance_service import reverse_split_on_refund
    with connect() as conn:
        reverse_split_on_refund(
            order_number, external_conn=conn, event_key=event_key,
            refund_amount=Decimal(str(amount)),
        )
        conn.commit()


def test_partial_refund_prorates_rain_points(refund_db):
    connect, state = refund_db
    buyer = _user(connect, state, true_total_points=100)
    order_no = 'TST' + uuid.uuid4().hex[:10]
    order_id = _order(connect, state, order_no, buyer)
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                'INSERT INTO rain_point_grants(flow_id,order_id,recipient_id,amount) '
                'VALUES(1,%s,%s,100)', (order_id, buyer))
        conn.commit()

    _refund(connect, order_no, uuid.uuid4().hex, 25)
    assert _value(connect, buyer, 'true_total_points') == Decimal('75.0000')

    _refund(connect, order_no, uuid.uuid4().hex, 75)
    assert _value(connect, buyer, 'true_total_points') == Decimal('0')


def test_full_refund_marks_grant_recovered(refund_db):
    connect, state = refund_db
    buyer = _user(connect, state, true_total_points=50)
    order_no = 'TST' + uuid.uuid4().hex[:10]
    order_id = _order(connect, state, order_no, buyer)
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                'INSERT INTO rain_point_grants(flow_id,order_id,recipient_id,amount) '
                'VALUES(1,%s,%s,50)', (order_id, buyer))
        conn.commit()

    _refund(connect, order_no, uuid.uuid4().hex, 100)

    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT state, recovered_refund_no FROM rain_point_grants WHERE flow_id=1')
            grant = cur.fetchone()
    assert grant['state'] == 'recovered'
    assert grant['recovered_refund_no']


def test_team_reward_reclaimed_from_both_fields(refund_db):
    connect, state = refund_db
    recipient = _user(connect, state, true_total_points=10, team_reward_points=10)
    order_no = 'TST' + uuid.uuid4().hex[:10]
    order_id = _order(connect, state, order_no, recipient)
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                'INSERT INTO team_reward_grants(flow_id,order_id,recipient_id,layer,amount) '
                'VALUES(1,%s,%s,1,10)', (order_id, recipient))
        conn.commit()

    _refund(connect, order_no, uuid.uuid4().hex, 100)

    assert _value(connect, recipient, 'team_reward_points') == Decimal('0')
    assert _value(connect, recipient, 'true_total_points') == Decimal('0')


def test_replay_is_idempotent(refund_db):
    connect, state = refund_db
    buyer = _user(connect, state, true_total_points=100)
    order_no = 'TST' + uuid.uuid4().hex[:10]
    order_id = _order(connect, state, order_no, buyer)
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                'INSERT INTO rain_point_grants(flow_id,order_id,recipient_id,amount) '
                'VALUES(1,%s,%s,100)', (order_id, buyer))
        conn.commit()

    key = uuid.uuid4().hex
    _refund(connect, order_no, key, 25)
    _refund(connect, order_no, key, 25)

    assert _value(connect, buyer, 'true_total_points') == Decimal('75.0000')


def test_coupon_voided_by_exact_split(refund_db):
    connect, state = refund_db
    buyer = _user(connect, state, true_total_points=0)
    order_no = 'TST' + uuid.uuid4().hex[:10]
    order_id = _order(connect, state, order_no, buyer)
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                'INSERT INTO rain_point_grants(flow_id,order_id,recipient_id,amount) '
                'VALUES(1,%s,%s,100)', (order_id, buyer))
            cur.execute(
                "INSERT INTO coupons(user_id,coupon_type,amount,status,valid_from,valid_to) "
                "VALUES(%s,'user',30,'unused',CURDATE(),DATE_ADD(CURDATE(),INTERVAL 30 DAY))",
                (buyer,))
        conn.commit()

    _refund(connect, order_no, uuid.uuid4().hex, 25)

    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT amount, status FROM coupons WHERE user_id=%s ORDER BY id', (buyer,))
            rows = cur.fetchall()
    unused = [r for r in rows if r['status'] == 'unused']
    expired = [r for r in rows if r['status'] == 'expired']
    assert len(unused) == 1 and Decimal(str(unused[0]['amount'])) == Decimal('5')
    assert len(expired) == 1 and Decimal(str(expired[0]['amount'])) == Decimal('25')


def test_other_order_is_not_touched(refund_db):
    connect, state = refund_db
    buyer = _user(connect, state, true_total_points=100)
    other = _user(connect, state, true_total_points=7)
    order_no = 'TST' + uuid.uuid4().hex[:10]
    other_no = 'TST' + uuid.uuid4().hex[:10]
    order_id = _order(connect, state, order_no, buyer)
    other_id = _order(connect, state, other_no, other, status='pending_recv')
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                'INSERT INTO rain_point_grants(flow_id,order_id,recipient_id,amount) '
                'VALUES(1,%s,%s,100)', (order_id, buyer))
            cur.execute(
                'INSERT INTO rain_point_grants(flow_id,order_id,recipient_id,amount) '
                'VALUES(2,%s,%s,7)', (other_id, other))
        conn.commit()

    _refund(connect, order_no, uuid.uuid4().hex, 100)

    assert _value(connect, other, 'true_total_points') == Decimal('7')


def _revoke(connect, order_no):
    from services.finance_service import FinanceService
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE orders SET status='refunded' WHERE order_number=%s", (order_no,))
        conn.commit()
    with connect() as conn:
        FinanceService.revoke_order_discounts(order_no, external_cur=conn.cursor())
        conn.commit()


def test_level_revert_uses_granted_level(refund_db):
    connect, state = refund_db
    buyer = _user(connect, state, member_level=6)
    order_no = 'TST' + uuid.uuid4().hex[:10]
    order_id = _order(connect, state, order_no, buyer)
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                'INSERT INTO member_level_grants(order_id,order_number,user_id,level_before,level_after,granted_level) '
                'VALUES(%s,%s,%s,5,6,1)', (order_id, order_no, buyer))
        conn.commit()

    _revoke(connect, order_no)

    assert _value(connect, buyer, 'member_level') == Decimal('5')


def test_level_revert_skips_capped_grant(refund_db):
    connect, state = refund_db
    buyer = _user(connect, state, member_level=6)
    order_no = 'TST' + uuid.uuid4().hex[:10]
    order_id = _order(connect, state, order_no, buyer)
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                'INSERT INTO member_level_grants(order_id,order_number,user_id,level_before,level_after,granted_level) '
                'VALUES(%s,%s,%s,6,6,0)', (order_id, order_no, buyer))
        conn.commit()

    _revoke(connect, order_no)

    assert _value(connect, buyer, 'member_level') == Decimal('6')