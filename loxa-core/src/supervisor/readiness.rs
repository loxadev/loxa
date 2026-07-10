use super::SupervisorError;
use std::net::{SocketAddr, TcpListener};

pub struct LocalhostPortReservation {
    listener: TcpListener,
    port: u16,
}

pub fn reserve_localhost_port(
    requested: Option<u16>,
) -> Result<LocalhostPortReservation, SupervisorError> {
    let address = SocketAddr::from(([127, 0, 0, 1], requested.unwrap_or(0)));
    let listener = TcpListener::bind(address).map_err(|_| SupervisorError::NoFreePort)?;
    let port = listener
        .local_addr()
        .map_err(|_| SupervisorError::NoFreePort)?
        .port();
    Ok(LocalhostPortReservation { listener, port })
}

impl LocalhostPortReservation {
    pub fn port(&self) -> u16 {
        self.port
    }

    pub(super) fn release_for(self, expected: u16) -> Result<(), SupervisorError> {
        if self.port != expected {
            return Err(SupervisorError::RunStateConflict(format!(
                "reserved port {} does not match spawn port {expected}",
                self.port
            )));
        }
        drop(self.listener);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    #[test]
    fn reservation_blocks_second_bind_until_consumed_at_spawn_boundary() {
        let reservation = reserve_localhost_port(None).expect("reserve localhost port");
        let address = SocketAddr::from(([127, 0, 0, 1], reservation.port()));

        assert!(TcpListener::bind(address).is_err());
        reservation
            .release_for(address.port())
            .expect("consume matching reservation");
        let rebound = TcpListener::bind(address).expect("rebind released localhost port");

        assert_eq!(rebound.local_addr().expect("rebound address"), address);
    }

    #[test]
    fn requested_port_reservation_uses_the_exact_requested_port() {
        let requested = TcpListener::bind(("127.0.0.1", 0))
            .expect("choose requested localhost port")
            .local_addr()
            .expect("requested localhost address")
            .port();

        let reservation =
            reserve_localhost_port(Some(requested)).expect("reserve requested localhost port");

        assert_ne!(requested, 0);
        assert_eq!(reservation.port(), requested);
    }

    #[test]
    fn reservation_port_mismatch_fails_before_os_spawn() {
        let reservation = reserve_localhost_port(None).expect("reserve localhost port");
        let mismatched_port = if reservation.port() == u16::MAX {
            reservation.port() - 1
        } else {
            reservation.port() + 1
        };
        let os_spawn_count = Cell::new(0_u8);

        let error = (|| -> Result<(), super::super::SupervisorError> {
            reservation.release_for(mismatched_port)?;
            os_spawn_count.set(os_spawn_count.get() + 1);
            Ok(())
        })()
        .expect_err("mismatched reservation must fail before OS spawn");

        assert!(matches!(
            error,
            super::super::SupervisorError::RunStateConflict(_)
        ));
        assert_eq!(os_spawn_count.get(), 0);
    }
}
