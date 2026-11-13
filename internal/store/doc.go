// Package store는 문자열 키와 값, 키별 만료 시각을 관리한다.
// primary는 AOF를 재생한 뒤 저장소를 단일 실행기에 넘긴다. replica는 받은
// 스냅샷으로 새 저장소를 완성한 뒤 단일 실행기에서 읽기용 키 공간으로 교체한다.
package store
