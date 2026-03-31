# 日本株ポジション管理 MVP

`Streamlit + SQLite` で動くローカル専用のポジション管理ツールです。

## 起動

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
streamlit run app.py
```

## MVPでできること

- 取引端末からコピーしたポジション一覧テキストを貼り付けて保存
- 日次スナップショットとしてSQLiteへ保存
- 前営業日の保存データと比較し、新規、解消、買い増し、売り増しを判定
- ロング、ショート別の件数、建玉金額、評価損益、実現損益、当日損益を集計
- 取り込み結果の確認と手動補正のための詳細テーブル表示

## 取り込み設計

- 1銘柄ごとに多数行が続く「ブロック形式」の貼り付けを想定
- ブロック開始は `名義行 -> 4桁/英字混在銘柄コード -> 銘柄名` を優先判定
- 数値は `,`、空欄、`--`、全角マイナスを吸収して正規化
- 固定列の完全一致には依存せず、ブロック内の候補値から優先順位で抽出
- 同一銘柄の複数行は `account_name + product_type + instrument_type + strategy_key + direction` で識別
- `strategy_key` は明示列がないため、MVPではブロック内容から算出したハッシュを使用

## SQLiteスキーマ

### `imports`

- 1回の貼り付け処理を記録
- `id`, `snapshot_date`, `imported_at`, `raw_text`, `record_count`, `note`

### `position_snapshots`

- 取り込み後の1ポジション1行
- `id`, `import_id`, `snapshot_date`, `account_name`, `code`, `name`, `instrument_type`, `product_type`, `currency`, `strategy_key`
- `qty_long`, `qty_short`, `net_qty`, `avg_price_long`, `avg_price_short`, `last_price`
- `market_value`, `unrealized_pl`, `realized_pl`, `day_pl`
- `source_block`, `created_at`

### 比較ロジック

- キー: `account_name + code + name + instrument_type + product_type + strategy_key`
- `prev.net_qty == 0 && curr.net_qty != 0` -> 新規
- `prev.net_qty != 0 && curr.net_qty == 0` -> 解消
- 同方向で絶対数量増加 -> 買い増し/売り増し
- 同方向で絶対数量減少 -> 一部解消
- 符号反転 -> ドテン

## 画面構成

### 1. 取込

- 対象日
- 貼り付けテキスト入力
- 任意メモ
- パース結果プレビュー
- 保存ボタン

### 2. 日次サマリ

- ロング/ショート別KPI
- 実現損益、評価損益、当日損益
- コード別、方向別サマリ

### 3. 前日比較

- 新規、解消、買い増し、売り増し、一部解消、ドテン
- 明細テーブル

### 4. スナップショット履歴

- 保存済み日付一覧
- 選択日の明細表示

## 制約

- サンプルがブロック形式のため、列名ベースの厳密マッピングは未使用
- 取引端末の出力形式差分が大きい場合は、`parser.py` の候補ルール追加で対応
