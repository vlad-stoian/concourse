package runner

type MigrationsRunner interface {
	Run(name string) error
}
