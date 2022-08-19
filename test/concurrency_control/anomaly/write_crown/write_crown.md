# write_crown.pdf の訂正
- PDF のデータと実体の齟齬
  - write preserve preserve optimization を考案し、適用しているため、テストケースの epoch 配置と厳密にはずれている箇所がある。
  - p1-2: t3 abort.
  - p2-8, ltx1occ2ltx1: t4 abort
  - p2-13, ltx1occ1ltx2: t4 abort
  - p2-14, ltx2occ1ltx1: t4 abort