# 接口测试 XMind 导图说明文档

本文档说明如何为项目创建 XMind 导图，用于测试接口并追踪对应的数据变化。

## 一、XMind 导图结构建议

### 1. 第一层：模块分类
- 财务系统
- 用户中心
- 订单系统
- 商品管理
- 系统配置

### 2. 第二层：接口分组
每个模块下按功能分组（如：用户认证、地址管理、积分管理等）

### 3. 第三层：具体接口
每个接口包含：
- 接口路径
- 请求方法（GET/POST/PUT/DELETE）
- 请求参数
- 影响的数据表
- 数据变化说明

---

## 二、各模块接口及数据变化映射

### 📊 财务系统模块

#### 1. 初始化接口
**接口：** `POST /api/init`
- **影响表：** 所有表（创建表结构）
- **变化：** 初始化数据库表结构

#### 2. 周补贴发放
**接口：** `POST /api/subsidy/distribute`
- **影响表：**
  - `weekly_subsidy_records` - 新增补贴记录
  - `coupons` - 创建优惠券
  - `users.member_points` - 扣除积分
  - `finance_accounts` - 补贴池余额减少
- **变化说明：**
  - 为符合条件的用户发放周补贴（以优惠券形式）
  - 扣除用户积分
  - 补贴池资金减少

#### 3. 联创星级分红
**接口：** `POST /api/unilevel/dividend`
- **影响表：**
  - `user_unilevel` - 查询联创等级
  - `users.points` - 增加点数
  - `points_log` - 记录积分流水
  - `finance_accounts` - 分红池余额减少
- **变化说明：**
  - 根据联创等级发放分红点数
  - 记录积分变动流水

#### 4. 预存补贴资金
**接口：** `POST /api/subsidy/fund?amount=10000`
- **影响表：**
  - `finance_accounts.balance` - 补贴池余额增加
- **变化说明：** 手动预存补贴池资金

#### 5. 提现审核
**接口：** `PATCH /api/withdrawals/audit`
- **影响表：**
  - `withdrawals.status` - 更新提现状态
  - `withdrawals.audit_remark` - 记录审核备注
  - `users.withdrawable_balance` - 扣除可提现余额（审核通过时）
  - `account_flow` - 记录资金流水
- **变化说明：**
  - 审核通过：扣除用户可提现余额，记录流水
  - 审核拒绝：仅更新状态

#### 6. 奖励审核
**接口：** `POST /api/rewards/audit`
- **影响表：**
  - `pending_rewards.status` - 更新奖励状态
  - `users.promotion_balance` - 增加推广余额（审核通过）
  - `account_flow` - 记录资金流水
- **变化说明：**
  - 审核通过：发放推广余额，记录流水
  - 审核拒绝：仅更新状态

#### 7. 优惠券发放
**接口：** `POST /api/coupons/distribute?user_id=1&amount=100&coupon_type=user`
- **影响表：**
  - `coupons` - 新增优惠券记录
- **变化说明：** 直接发放优惠券给用户

#### 8. 优惠券使用
**接口：** `POST /api/coupons/use?coupon_id=1&user_id=1`
- **影响表：**
  - `coupons.status` - 更新为 'used'
  - `coupons.used_at` - 记录使用时间
- **变化说明：** 优惠券状态变为已使用

#### 9. 清空资金池
**接口：** `POST /api/fund-pools/clear`
- **影响表：**
  - `finance_accounts.balance` - 清空指定资金池余额
  - `account_flow` - 记录清空流水
- **变化说明：** 手动清空指定资金池

---

### 👤 用户中心模块

#### 1. 用户认证（登录/注册）
**接口：** `POST /user/auth`
- **影响表：**
  - `users` - 新用户注册时新增记录
  - `user_referrals` - 如果有推荐人，新增推荐关系
- **变化说明：**
  - 登录：仅查询，无数据变化
  - 注册：创建新用户，可能创建推荐关系

#### 2. 修改资料
**接口：** `POST /user/update-profile`
- **影响表：**
  - `users.name` - 更新姓名
  - `users.avatar_path` - 更新头像路径
  - `users.password_hash` - 更新密码（如果提供）
- **变化说明：** 更新用户基本信息

#### 3. 用户状态管理
**接口：** `POST /user/set-status`
- **影响表：**
  - `users.status` - 更新用户状态（0-正常 1-冻结 2-注销）
  - `audit_log` - 记录状态变更日志
- **变化说明：** 冻结/注销/恢复正常用户

#### 4. 用户升级
**接口：** `POST /user/upgrade?mobile=13800138000`
- **影响表：**
  - `users.member_level` - 星级+1
  - `users.level_changed_at` - 更新等级变更时间
  - `audit_log` - 记录升级日志
- **变化说明：** 用户升1星

#### 5. 后台调星
**接口：** `POST /user/set-level`
- **影响表：**
  - `users.member_level` - 设置指定星级
  - `users.level_changed_at` - 更新等级变更时间
  - `audit_log` - 记录调星日志
- **变化说明：** 后台直接设置用户星级

#### 6. 绑定推荐人
**接口：** `POST /user/bind-referrer`
- **影响表：**
  - `user_referrals` - 新增或更新推荐关系
  - `users.referral_id` - 更新推荐人ID
- **变化说明：** 建立用户推荐关系

#### 7. 地址管理
**接口：** `POST /address` - 新增地址
- **影响表：**
  - `addresses` - 新增地址记录
  - `addresses.is_default` - 取消其他默认地址（如果设为默认）
- **变化说明：** 新增用户地址

**接口：** `PUT /address/default?addr_id=1&mobile=13800138000` - 设为默认
- **影响表：**
  - `addresses.is_default` - 更新默认地址
- **变化说明：** 设置默认地址

**接口：** `DELETE /address/{addr_id}?mobile=13800138000` - 删除地址
- **影响表：**
  - `addresses` - 删除地址记录
- **变化说明：** 删除指定地址

#### 8. 积分管理
**接口：** `POST /points` - 增减积分
- **影响表：**
  - `users.member_points` - 更新会员积分
  - `users.merchant_points` - 更新商家积分
  - `points_log` - 记录积分流水
- **变化说明：** 增加或扣除积分，记录流水

#### 9. 赋予商户身份
**接口：** `POST /user/grant-merchant?mobile=13800138000&admin_key=gm2025`
- **影响表：**
  - `users.is_merchant` - 设置为1（商户）
- **变化说明：** 赋予用户商户身份

#### 10. 晋升联创
**接口：** `POST /admin/unilevel/promote?user_id=1&level=1&admin_key=gm2025`
- **影响表：**
  - `user_unilevel` - 新增或更新联创等级记录
- **变化说明：** 晋升用户为联创（1-3星）

#### 11. 上传头像
**接口：** `POST /user/{user_id}/avatar`
- **影响表：**
  - `users.avatar_path` - 更新头像路径
- **变化说明：** 上传并更新用户头像

---

### 🛒 订单系统模块

#### 1. 购物车管理
**接口：** `POST /cart/add` - 添加到购物车
- **影响表：**
  - `cart` - 新增或更新购物车记录
- **变化说明：** 添加商品到购物车

**接口：** `PUT /cart/update` - 更新购物车
- **影响表：**
  - `cart.quantity` - 更新数量
  - `cart.selected` - 更新选中状态
- **变化说明：** 更新购物车商品数量或选中状态

**接口：** `DELETE /cart/remove` - 删除购物车
- **影响表：**
  - `cart` - 删除购物车记录
- **变化说明：** 从购物车删除商品

#### 2. 创建订单
**接口：** `POST /order/create`
- **影响表：**
  - `orders` - 新增订单记录
  - `order_items` - 新增订单明细
  - `cart` - 清空已选中的购物车（如果从购物车创建）
  - `users.member_points` - 扣除积分（如果使用积分抵扣）
  - `points_log` - 记录积分抵扣流水
  - `account_flow` - 记录订单资金拆分流水
  - `finance_accounts` - 更新各资金池余额
  - `pending_rewards` - 创建待审核奖励记录
- **变化说明：**
  - 创建订单和订单明细
  - 如果使用积分，扣除积分并记录流水
  - 订单资金拆分到各资金池
  - 创建推荐奖励和团队奖励（待审核）

#### 3. 订单支付
**接口：** `POST /order/pay`
- **影响表：**
  - `orders.status` - 更新为 'pending_ship'
- **变化说明：** 订单状态变为待发货

#### 4. 订单发货
**接口：** `POST /order/ship`
- **影响表：**
  - `orders.status` - 更新为 'pending_recv'
  - `orders.tracking_number` - 更新快递单号
  - `orders.auto_recv_time` - 设置自动收货时间（7天后）
- **变化说明：** 订单发货，设置快递单号和自动收货时间

#### 5. 订单确认收货
**接口：** `POST /order/confirm-receive`
- **影响表：**
  - `orders.status` - 更新为 'completed'
- **变化说明：** 订单完成

#### 6. 订单退款
**接口：** `POST /refund/apply` - 申请退款
- **影响表：**
  - `refunds` - 新增退款记录
  - `orders.refund_status` - 更新退款状态
- **变化说明：** 创建退款申请

**接口：** `POST /refund/approve` - 同意退款
- **影响表：**
  - `refunds.status` - 更新为 'success'
  - `orders.refund_status` - 更新为 'refunded'
  - `orders.status` - 更新订单状态
  - `users.member_points` - 退回积分（如果使用了积分）
  - `points_log` - 记录积分退回流水
  - `account_flow` - 记录退款回冲流水
  - `finance_accounts` - 各资金池余额回冲
- **变化说明：**
  - 退款成功，退回积分
  - 各资金池回冲资金

#### 7. 商家对账单
**接口：** `GET /merchant/statement`
- **影响表：**
  - `merchant_statement` - 查询对账单（可能自动生成）
- **变化说明：** 查询商家对账单

---

### 📦 商品管理模块

#### 1. 创建商品
**接口：** `POST /api/products`
- **影响表：**
  - `products` - 新增商品记录
  - `product_skus` - 新增SKU记录
  - `product_attributes` - 新增商品属性（如果有）
- **变化说明：** 创建新商品及其SKU和属性

#### 2. 更新商品
**接口：** `PUT /api/products/{id}`
- **影响表：**
  - `products` - 更新商品信息
  - `product_skus` - 更新SKU信息
  - `product_attributes` - 更新商品属性
- **变化说明：** 更新商品信息

#### 3. 上传商品图片
**接口：** `POST /api/products/{id}/images`
- **影响表：**
  - `products.main_image` - 更新主图
  - `products.detail_images` - 更新详情图
- **变化说明：** 上传并更新商品图片

#### 4. 轮播图管理
**接口：** `POST /api/banners` - 新增轮播图
- **影响表：**
  - `banner` - 新增轮播图记录
- **变化说明：** 添加轮播图

**接口：** `PUT /api/banners/{id}` - 更新轮播图
- **影响表：**
  - `banner` - 更新轮播图信息
- **变化说明：** 更新轮播图

**接口：** `DELETE /api/banners/{id}` - 删除轮播图
- **影响表：**
  - `banner` - 删除轮播图记录
- **变化说明：** 删除轮播图

---

### ⚙️ 系统配置模块

#### 1. 获取系统标语
**接口：** `GET /api/system/sentences`
- **影响表：**
  - `system_sentence` - 查询系统标语（如果不存在会自动创建）
- **变化说明：** 查询系统标语，不存在时自动创建空记录

#### 2. 更新系统标语
**接口：** `PUT /api/system/sentences`
- **影响表：**
  - `system_sentence` - 更新系统标语
- **变化说明：** 更新轮播图标语和系统标语

---

## 三、XMind 导图制作步骤

### 步骤1：创建中心主题
- 主题：**"接口测试数据变化追踪"**

### 步骤2：创建第一层分支（模块）
1. 财务系统
2. 用户中心
3. 订单系统
4. 商品管理
5. 系统配置

### 步骤3：为每个模块创建子分支（接口分组）
例如：
- **财务系统**
  - 初始化
  - 补贴管理
  - 分红管理
  - 提现管理
  - 奖励管理
  - 优惠券管理
  - 资金池管理
  - 报表查询

### 步骤4：为每个接口创建详细节点
每个接口节点包含：
- **接口路径**：如 `POST /api/subsidy/distribute`
- **请求参数**：列出主要参数
- **影响表**：列出所有会变化的数据表
- **数据变化**：详细说明每条记录的变化

### 步骤5：使用颜色标记
- 🔴 红色：写操作（INSERT/UPDATE/DELETE）
- 🟢 绿色：读操作（SELECT）
- 🟡 黄色：复杂操作（涉及多表）

### 步骤6：添加测试用例
在每个接口节点下添加：
- 测试场景
- 预期结果
- 验证SQL（用于验证数据变化）

---

## 四、测试验证SQL示例

### 测试周补贴发放后验证
```sql
-- 查看补贴记录
SELECT * FROM weekly_subsidy_records WHERE user_id = 1 ORDER BY id DESC LIMIT 1;

-- 查看生成的优惠券
SELECT * FROM coupons WHERE user_id = 1 ORDER BY id DESC LIMIT 1;

-- 查看积分变化
SELECT * FROM points_log WHERE user_id = 1 AND type = 'member' ORDER BY id DESC LIMIT 1;

-- 查看补贴池余额
SELECT balance FROM finance_accounts WHERE account_type = 'subsidy_pool';
```

### 测试订单创建后验证
```sql
-- 查看订单
SELECT * FROM orders WHERE order_number = 'ORDER123';

-- 查看订单明细
SELECT * FROM order_items WHERE order_id = 1;

-- 查看资金拆分流水
SELECT * FROM account_flow WHERE remark LIKE '%订单分账: ORDER123%';

-- 查看待审核奖励
SELECT * FROM pending_rewards WHERE order_id = 1;
```

---

## 五、XMind 导图模板结构

```
接口测试数据变化追踪
├── 财务系统
│   ├── 初始化
│   │   └── POST /api/init
│   │       ├── 影响表：所有表
│   │       └── 变化：创建表结构
│   ├── 补贴管理
│   │   ├── POST /api/subsidy/distribute
│   │   │   ├── 影响表：weekly_subsidy_records, coupons, users, finance_accounts
│   │   │   └── 变化：发放补贴、扣除积分、创建优惠券
│   │   └── POST /api/subsidy/fund
│   │       ├── 影响表：finance_accounts
│   │       └── 变化：预存补贴资金
│   ├── 提现管理
│   │   └── PATCH /api/withdrawals/audit
│   │       ├── 影响表：withdrawals, users, account_flow
│   │       └── 变化：审核提现、扣除余额
│   └── ...
├── 用户中心
│   ├── 用户认证
│   │   └── POST /user/auth
│   │       ├── 影响表：users, user_referrals
│   │       └── 变化：注册新用户、创建推荐关系
│   ├── 地址管理
│   │   ├── POST /address
│   │   ├── PUT /address/default
│   │   └── DELETE /address/{addr_id}
│   └── ...
├── 订单系统
│   ├── 购物车
│   │   ├── POST /cart/add
│   │   ├── PUT /cart/update
│   │   └── DELETE /cart/remove
│   ├── 订单管理
│   │   ├── POST /order/create
│   │   ├── POST /order/pay
│   │   ├── POST /order/ship
│   │   └── POST /order/confirm-receive
│   └── ...
├── 商品管理
│   ├── POST /api/products
│   ├── PUT /api/products/{id}
│   └── POST /api/products/{id}/images
│   └── ...
└── 系统配置
    ├── GET /api/system/sentences
    └── PUT /api/system/sentences
```

---

## 六、使用建议

1. **测试前准备**
   - 在测试数据库中执行初始化
   - 准备测试数据（用户、商品等）

2. **测试执行**
   - 按照 XMind 导图顺序测试接口
   - 每个接口测试后立即验证数据变化

3. **数据验证**
   - 使用提供的验证SQL检查数据
   - 对比预期结果和实际结果

4. **问题追踪**
   - 在 XMind 中标记有问题的接口
   - 记录异常情况和错误信息

5. **文档更新**
   - 发现新的数据变化时，及时更新导图
   - 记录测试中发现的问题和解决方案

---

## 七、快速参考表

| 模块 | 主要数据表 | 关键字段 |
|------|-----------|---------|
| 财务系统 | finance_accounts, account_flow, pending_rewards, coupons, withdrawals | balance, change_amount, status |
| 用户中心 | users, user_referrals, addresses, points_log | member_level, status, member_points, merchant_points |
| 订单系统 | orders, order_items, cart, refunds | status, total_amount, points_discount |
| 商品管理 | products, product_skus, product_attributes, banner | status, price, stock |
| 系统配置 | system_sentence | banner_sentence, system_sentence |

---

**注意：** 此文档基于当前代码结构生成，如有接口变更，请及时更新此文档和 XMind 导图。

