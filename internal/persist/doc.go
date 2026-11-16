// Package persist는 키 공간의 변경을 AOF(Append Only File) 끝에 순서대로 기록한다.
// 서버가 다시 시작되면 같은 파일을 앞에서부터 읽어 값과 절대 만료 시각을 복구한다.
package persist
