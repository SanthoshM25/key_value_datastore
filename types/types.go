package types

const MaxBatchLimit = 4 * 1024 * 1024

type User struct {
	ID                  int64  `json:"id"`
	Name                string `json:"user_name"`
	Password            string `json:"password"`
	ProvisionedCapacity int64  `json:"provisioned_capacity"` // represents the capactiy in B
}

type Object struct {
	Key   string `json:"key"`
	Value any    `json:"value"`
	TTL   int64  `json:"ttl"`
}

type Quota struct {
	Provisioned int64 `json:"provisioned"`
	Utilised    int64 `json:"utilised"`
}
