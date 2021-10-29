Application.load(:cicadabus)

for app <- Application.spec(:cicadabus, :applications) || [] do
  Application.ensure_all_started(app)
end

ExUnit.configure(exclude: [unimplemented: true])

ExUnit.start()
