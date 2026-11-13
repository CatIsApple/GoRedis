package store

// Store는 단일 실행기가 소유하는 인메모리 키 공간이다.
// 키와 값은 임의의 바이트를 보존하는 Go 문자열로 저장한다.
type Store struct {
	values map[string]string
}

func New() *Store {
	return &Store{values: make(map[string]string)}
}

func (s *Store) Set(key, value string) {
	s.values[key] = value
}

func (s *Store) Get(key string) (string, bool) {
	value, found := s.values[key]
	return value, found
}
