(ns quanta.bar.db)

(defprotocol bar-db
  ;(get-bars [this opts window])
  ;(append-bars [this opts ds-bars])
  (delete-bars [this opts])
  (summary [this opts]))