package cluster

import "strings"

const slotCount = 16384

// CRC-16/XMODEM은 초기값 0, 다항식 0x1021을 사용한다. 바이트 하나를
// 처리할 때마다 8번 반복하는 대신 256개 바이트의 계산 결과를 재사용한다.
var crcTable = func() (table [256]uint16) {
	for i := range table {
		crc := uint16(i) << 8
		for range 8 {
			if crc&0x8000 != 0 {
				crc = crc<<1 ^ 0x1021
			} else {
				crc <<= 1
			}
		}
		table[i] = crc
	}
	return table
}()

// KeySlot은 Redis와 같은 해시 태그 규칙을 적용한다. 첫 '{' 뒤 첫 '}'까지
// 비어 있으면 키 전체를 쓴다. 그 뒤에 또 다른 태그가 있어도 다시 찾지 않는다.
func KeySlot(key string) int {
	if open := strings.IndexByte(key, '{'); open >= 0 {
		tail := key[open+1:]
		if end := strings.IndexByte(tail, '}'); end > 0 {
			key = tail[:end]
		}
	}
	var crc uint16
	for i := 0; i < len(key); i++ {
		crc = crc<<8 ^ crcTable[byte(crc>>8)^key[i]]
	}
	return int(crc) % slotCount
}

// slot*n/slotCount로 샤드를 찾는다. 이 식을 역으로 계산하면 샤드 i의
// 첫 슬롯은 ceil(i*slotCount/n)이고 마지막은 다음 샤드의 첫 슬롯 바로 전이다.
func firstSlot(i, n int) int {
	return (i*slotCount + n - 1) / n
}
