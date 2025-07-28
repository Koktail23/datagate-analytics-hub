import reflex as rx

def navbar() -> rx.Component:
    """Навигационная панель приложения."""
    return rx.box(
        rx.hstack(
            rx.link(
                rx.heading("DataGate Analytics", size="6"),
                href="/",
                text_decoration="none",
                color="inherit",
            ),
            rx.spacer(),
            rx.hstack(
                rx.link(
                    rx.button("Валидатор", variant="ghost"),
                    href="/validator",
                ),
                rx.link(
                    rx.button("Dashboard", variant="ghost"),
                    href="/dashboard",
                ),
                rx.link(
                    rx.button("Profiler", variant="ghost"),
                    href="/profiler",
                ),
                spacing="2",
            ),
            width="100%",
            padding="1rem",
            align="center",
        ),
        background="var(--gray-2)",
        border_bottom="1px solid var(--gray-4)",
        position="sticky",
        top="0",
        z_index="10",
    )