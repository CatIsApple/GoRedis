# GoRedis

Go로 Redis를 구현하는 학습 프로젝트입니다. 현재 진입점을 실행하면 `GoRedis`를 출력하고 종료합니다.

## 개발 환경

Go 1.26.0 이상에서 저장소 루트를 기준으로 실행합니다.

```sh
go run ./cmd/goredis   # 실행
go build ./cmd/goredis # 루트에 goredis 실행 파일 생성
go test ./...          # 전체 패키지 테스트
go vet ./...           # 정적 검사
```
