/**
 *
 */
module org.jboss.threads.virtual {
    requires static org.jboss.logging.annotations;

    requires io.smallrye.common.constraint;

    requires org.jboss.logging;
    requires org.jboss.threads;

    exports org.jboss.threads.virtual;
}