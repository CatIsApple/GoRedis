// Package persist는 문자열·해시·절대 만료 시각을 Redis RDB에 저장하고,
// 그 시점 이후의 변경을 증분 AOF에 기록한다. 복구 순서는 manifest에서 읽는다.
package persist
